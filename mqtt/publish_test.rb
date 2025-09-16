require 'mqtt'
require 'json'

# MQTT broker details (adjust for your setup)
mqtt_host = 'localhost' # Default Meshtastic public server
mqtt_port = 1883
# mqtt_username = 'meshdev' # Default username for public server
# mqtt_password = 'large4cats' # Default password for public server

# Topic for sending JSON messages to Meshtastic (adjust REGION if needed)
# This example sends a message to a channel named 'mqtt'
mqtt_topic = 'meshtastic/receive' # Example for US region, JSON topic for 'mqtt' channel

# Message payload (adjust 'to' and 'text' as needed)
message_payload = {
  to: '^all', # Replace with the Node ID of the target Meshtastic device (e.g., !YOUR_NODE_ID)
  text: 'Hello from Ruby via MQTT!'
}.to_json

begin
  # Connect to the MQTT broker
  client = MQTT::Client.connect(
    host: mqtt_host,
    port: mqtt_port
    #,
    #username: mqtt_username,
    #password: mqtt_password
  )

  puts "Connected to MQTT broker: #{mqtt_host}"

  # Publish the message
  client.publish(mqtt_topic, message_payload)
  puts "Message published to topic: #{mqtt_topic}"
  puts "Payload: #{message_payload}"

rescue MQTT::ProtocolException => e
  puts "MQTT Protocol Error: #{e.message}"
rescue StandardError => e
  puts "An error occurred: #{e.message}"
ensure
  # Disconnect from the MQTT broker
  client.disconnect if client
  puts "Disconnected from MQTT broker."
end