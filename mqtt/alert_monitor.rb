#!/usr/bin/env ruby

require 'mqtt'
require 'json'
require 'sqlite3'
require 'logger'
require 'inifile'
require 'set'
require 'fileutils'

# Make sure the notifications module can be found relative to this script's location.
project_root = File.expand_path('../..', __FILE__)
$LOAD_PATH.unshift(project_root)
require 'notifications/alert_dispatch'

# Main class to encapsulate the monitoring logic.
class AlertMonitor
  def initialize(config_path)
    @config = load_app_config(config_path)
    @logger = setup_logging
    @whitelist, @node_locations = load_data_files
    @db = setup_database

    @ema_states = {}
    @message_counter = 0
    @cleanup_interval = 100

    @logger.info('Monitor initialized successfully.')
  end

  def run
    mqtt_host = @config['MQTT']['Host']
    mqtt_port = @config['MQTT']['Port'].to_i
    mqtt_topic = @config['MQTT']['Topic']

    @logger.info("Attempting to connect to MQTT broker at #{mqtt_host}:#{mqtt_port}...")
    client = MQTT::Client.new(host: mqtt_host, port: mqtt_port)
    client.connect

    @logger.info('Connected successfully to MQTT Broker.')
    client.subscribe(mqtt_topic)
    @logger.info("Subscribed to topic: #{mqtt_topic}")

    # Main message loop
    client.get do |topic, message|
      on_message(topic, message)
    end
  rescue MQTT::Connack::ConnectionRefused => e
    @logger.error("MQTT connection refused. Is the broker running at #{mqtt_host}:#{mqtt_port}? Details: #{e.message}")
  rescue SystemCallError => e
    @logger.error("Network error connecting to MQTT broker: #{e.message}")
  rescue Interrupt
    @logger.info('Script interrupted by user.')
  rescue StandardError => e
    @logger.fatal("An unexpected error occurred in the main loop: #{e.class} - #{e.message}")
    @logger.fatal(e.backtrace.join("\n"))
  ensure
    @logger.info('Shutting down...')
    client.disconnect if client&.connected?
    if @db
      @logger.info('Committing final batch and closing database connection...')
      @db.commit if @db.transaction_active?
      @db.close
    end
    @logger.info('Script finished.')
  end

  private

  def on_message(topic, message)
    puts "topic: #{topic}"
    puts "message: #{message}"

    # --- Periodic Cleanup & Commit ---
    @message_counter += 1
    if @message_counter >= @cleanup_interval
      cleanup_ema_states
      @db.commit if @db&.transaction_active?
      @logger.debug("Committed pending detections to database.")
      @message_counter = 0
    end

    # --- Message Handling Pipeline ---
    parsed_data = parse_mqtt_message(message, topic)
    return unless parsed_data

    status = process_detection(parsed_data)
    trigger_alert_if_needed(parsed_data['mac'], parsed_data['node_id'], status)
  rescue StandardError => e
    @logger.error("Error in on_message handler: #{e.message}")
    @logger.error(e.backtrace.join("\n"))
  end

  def parse_mqtt_message(payload_bytes, topic)
    rssi_min_threshold = @config['Filtering']['RSSIMin'].to_i

    payload = JSON.parse(payload_bytes)
    mac = payload['mac']&.strip&.upcase
    rssi = payload['rssi'].to_i

    unless mac && !mac.empty?
      @logger.debug('Message missing MAC address, skipping.')
      return nil
    end

    if rssi < rssi_min_threshold
      @logger.debug("Signal from #{mac} (#{rssi} dBm) below threshold (#{rssi_min_threshold} dBm), skipping.")
      return nil
    end

    {
      'mac' => mac,
      'node_id' => payload['from'].to_s,
      'rssi' => rssi,
      'timestamp_iso' => Time.now.utc.iso8601
    }
  rescue JSON::ParserError
    @logger.warn("Received non-JSON message on #{topic}: #{payload_bytes[0..80]}...")
    nil
  rescue StandardError => e
    @logger.error("Unexpected error parsing message payload: #{e.message}")
    nil
  end

  def process_detection(detection_data)
    mac = detection_data['mac']
    node_id = detection_data['node_id']

    smoothed_rssi = exponential_moving_average(mac, detection_data['rssi'])
    status = @whitelist.include?(mac) ? 'whitelisted' : 'unknown'

    node_info = @node_locations[node_id] || {}
    lat = node_info['lat']
    lon = node_info['lon']

    log_to_sqlite(mac, node_id, smoothed_rssi, detection_data['timestamp_iso'], lat, lon)
    @logger.info("Processed: MAC=#{mac}, Node=#{node_id}, RSSI=#{smoothed_rssi.round(1)}, Status=#{status}, Loc=(#{lat},#{lon})")

    status
  end

  def trigger_alert_if_needed(mac, node_id, status)
    return unless status == 'unknown'

    @logger.warn("Unknown MAC detected: #{mac} from Node #{node_id}. Sending alert.")
    AlertDispatch.send_alert(mac, node_id, @config, @logger)
  end

  def exponential_moving_average(mac, value)
    ema_alpha = @config['Filtering']['EMAAlpha'].to_f
    now_ts = Time.now.to_f

    unless @ema_states.key?(mac)
      @ema_states[mac] = { value: value, timestamp: now_ts }
      return value
    end

    current_ema = @ema_states[mac][:value]
    smoothed_value = (ema_alpha * value) + ((1 - ema_alpha) * current_ema)
    @ema_states[mac] = { value: smoothed_value, timestamp: now_ts }

    smoothed_value
  end

  def cleanup_ema_states
    timeout_seconds = @config['Filtering']['StateTimeoutSeconds'].to_i
    now_ts = Time.now.to_f
    expired_macs = @ema_states.select { |_, state| now_ts - state[:timestamp] > timeout_seconds }.keys

    unless expired_macs.empty?
      expired_macs.each { |mac| @ema_states.delete(mac) }
      @logger.info("Cleaned up EMA state for #{expired_macs.length} expired MAC(s).")
    end
  end

  def log_to_sqlite(mac, node, smoothed_rssi, timestamp_iso, lat, lon)
    return unless @db

    # Start a transaction if one isn't already active
    @db.transaction if !@db.transaction_active?

    @db.execute(
      'INSERT INTO detections (mac, node, rssi, timestamp, lat, lon) VALUES (?, ?, ?, ?, ?, ?)',
      [mac, node, smoothed_rssi, timestamp_iso, lat, lon]
    )
  rescue SQLite3::Exception => e
    @logger.error("Failed to execute insert for MAC #{mac} to SQLite: #{e.message}")
  end

  # --- Setup Methods ---

  def load_app_config(path)
    unless File.exist?(path)
      raise "Configuration file not found: #{path}"
    end
    IniFile.load(path)
  end

  def setup_logging
    level_str = @config['Logging']['Level'].upcase
    level = Logger::Severity.const_get(level_str) rescue Logger::INFO

    logger = Logger.new(STDOUT)
    logger.level = level
    logger.formatter = proc do |severity, datetime, _, msg|
      "#{datetime.strftime('%Y-%m-%d %H:%M:%S')} - #{severity} - #{msg}\n"
    end
    logger.info("Logging configured to level #{level_str}")
    logger
  end

  def load_data_files
    whitelist_file = @config['Files']['Whitelist']
    nodes_file = @config['Files']['Nodes']

    # Load whitelist
    whitelist = Set.new
    begin
      File.foreach(whitelist_file) { |line| whitelist.add(line.strip.upcase) if line.strip != '' }
      @logger.info("Loaded #{whitelist.size} MACs from #{whitelist_file}")
    rescue Errno::ENOENT
      @logger.warn("Whitelist file not found: #{whitelist_file}. Proceeding with empty whitelist.")
    end

    # Load node locations
    node_locations = {}
    begin
      node_locations = JSON.parse(File.read(nodes_file))
      @logger.info("Loaded #{node_locations.size} node locations from #{nodes_file}")
    rescue Errno::ENOENT
      @logger.warn("Nodes file not found: #{nodes_file}. Proceeding with empty node locations.")
    rescue JSON::ParserError
      @logger.error("Error decoding JSON from #{nodes_file}. Proceeding with empty node locations.")
    end

    [whitelist, node_locations]
  end

  def setup_database
    db_path = @config['Files']['Database']
    db_dir = File.dirname(db_path)
    FileUtils.mkdir_p(db_dir) unless Dir.exist?(db_dir)

    db = SQLite3::Database.new(db_path)
    db.execute <<-SQL
      CREATE TABLE IF NOT EXISTS detections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          mac TEXT NOT NULL,
          node TEXT,
          rssi REAL,
          timestamp TEXT NOT NULL,
          lat REAL,
          lon REAL
      );
    SQL
    @logger.info("Connected to SQLite database: #{db_path}")
    db
  rescue SQLite3::Exception => e
    @logger.error("Database error connecting to #{db_path}: #{e.message}")
    nil
  rescue SystemCallError => e
    @logger.error("OS error setting up database directory #{db_path}: #{e.message}")
    nil
  end
end

# --- Main Execution ---
if __FILE__ == $PROGRAM_NAME
  begin
    config_path = File.join(project_root, 'config', 'config.ini')
    monitor = AlertMonitor.new(config_path)
    monitor.run
  rescue StandardError => e
    # This catches errors during initialization
    puts "FATAL: Failed to initialize monitor: #{e.message}"
    exit 1
  end
end