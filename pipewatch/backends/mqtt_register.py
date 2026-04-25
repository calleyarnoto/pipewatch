"""Register the MQTT backend in the global backend registry."""
from pipewatch.backends import register_backend
from pipewatch.backends.mqtt import MQTTBackend

register_backend("mqtt", MQTTBackend)
