require 'securerandom'

module PulsarSdk
  module Options
    class Base
      prepend ::PulsarSdk::Tweaks::AssignAttributes
      prepend ::PulsarSdk::Tweaks::CleanInspect
    end
  end
end
