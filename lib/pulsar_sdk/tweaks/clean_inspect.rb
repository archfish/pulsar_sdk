module PulsarSdk
  module Tweaks
    module CleanInspect
      def inspect
        s = instance_variables.map do |x|
          v = instance_variable_get(x)
          next if v.is_a?(Proc)
          "#{x}: #{v}"
        end.join(', ')

        "<#{self.class.name} #{s}>"
      end
    end
  end
end
