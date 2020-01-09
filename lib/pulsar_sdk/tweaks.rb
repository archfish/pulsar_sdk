require 'pulsar_sdk/tweaks/assign_attributes'
require 'pulsar_sdk/tweaks/clean_inspect'
require 'pulsar_sdk/tweaks/base_command'
require 'pulsar_sdk/tweaks/time_at_microsecond'
require 'pulsar_sdk/tweaks/timeout_queue'
require 'pulsar_sdk/tweaks/wait_map'
require 'pulsar_sdk/tweaks/binary_heap'

# 扩展type的判断方法，方便书写，统一以 typeof_ 开头
Pulsar::Proto::BaseCommand.prepend PulsarSdk::Tweaks::BaseCommand
