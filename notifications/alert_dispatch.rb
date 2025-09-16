require 'httparty'
require 'json'

# This module handles dispatching alerts to various notification services.
module AlertDispatch
  def self.send_alert(mac, node_id, app_config, logger)
    message = "Unknown MAC address detected: #{mac}\nSensed by node: #{node_id}"

    # --- Twilio SMS Alert ---
    if app_config.get('Notifications', 'EnableTwilio').to_s.downcase == 'true'
      send_twilio_alert(message, app_config, logger)
    end

    # --- ntfy.sh Alert ---
    if app_config.get('Notifications', 'EnableNtfy').to_s.downcase == 'true'
      send_ntfy_alert(message, app_config, logger)
    end

    # --- Webhook Alert ---
    if app_config.get('Notifications', 'EnableWebhook').to_s.downcase == 'true'
      send_webhook_alert(message, app_config, logger)
    end
  end

  # private

  def self.send_twilio_alert(message, app_config, logger)
    account_sid = app_config.get('Notifications', 'TwilioAccountSID')
    auth_token = app_config.get('Notifications', 'TwilioAuthToken')
    from_phone = app_config.get('Notifications', 'TwilioFromPhone')
    to_phone = app_config.get('Notifications', 'TwilioToPhone')

    unless account_sid && auth_token && from_phone && to_phone && account_sid != 'ACXXXXXXXXXXXXXXXXX'
      logger.warn('Twilio enabled but one or more required settings are missing or default in config.')
      return
    end

    twilio_url = "https://api.twilio.com/2010-04-01/Accounts/#{account_sid}/Messages.json"
    response = HTTParty.post(
      twilio_url,
      basic_auth: { username: account_sid, password: auth_token },
      body: { 'From' => from_phone, 'To' => to_phone, 'Body' => message }
    )
    response.success? ? logger.info("Sent alert via Twilio SMS to #{to_phone}") : logger.error("Failed to send Twilio alert. Status: #{response.code}, Body: #{response.body}")
  rescue StandardError => e
    logger.error("Unexpected error sending Twilio SMS: #{e.message}")
  end

  def self.send_ntfy_alert(message, app_config, logger)
    ntfy_topic = app_config.get('Notifications', 'NtfyTopic')
    unless ntfy_topic
      logger.warn('Ntfy enabled but NtfyTopic is missing in config.')
      return
    end

    ntfy_url = "https://ntfy.sh/#{ntfy_topic}"
    response = HTTParty.post(ntfy_url, body: message, headers: { 'Title' => 'Tripwire Alert' })
    response.success? ? logger.info("Sent alert via ntfy.sh to topic #{ntfy_topic}") : logger.error("Failed to send ntfy alert. Status: #{response.code}, Body: #{response.body}")
  rescue StandardError => e
    logger.error("Unexpected error sending ntfy alert: #{e.message}")
  end

  def self.send_webhook_alert(message, app_config, logger)
    webhook_url = app_config.get('Notifications', 'WebhookURL')
    unless webhook_url && webhook_url.start_with?('http')
      logger.warn('Webhook enabled but WebhookURL is missing or invalid in config.')
      return
    end

    response = HTTParty.post(
      webhook_url,
      body: { content: message }.to_json,
      headers: { 'Content-Type' => 'application/json' }
    )
    response.success? ? logger.info("Sent alert via Webhook to #{webhook_url}") : logger.error("Failed to send webhook alert. Status: #{response.code}, Body: #{response.body}")
  rescue StandardError => e
    logger.error("Unexpected error sending webhook alert: #{e.message}")
  end

  private_class_method :send_twilio_alert, :send_ntfy_alert, :send_webhook_alert
end