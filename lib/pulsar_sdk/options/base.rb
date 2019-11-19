require 'securerandom'

module PulsarSdk
  module Options
    class Base
      prepend ::PulsarSdk::Tweaks::AssignAttributes
    end
  end
end
