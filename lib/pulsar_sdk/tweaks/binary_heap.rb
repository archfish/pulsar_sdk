module PulsarSdk
  module Tweaks
    class BinaryHeap
      attr_reader :data

      # default is max heap, custom compare lambda to build your sort
      def initialize(ary=nil, &block)
        @cmp = block || lambda{|parent, child| parent <=> child }
        @mutex = Mutex.new

        raise "type of ary must be Array or its subclass" unless ary.nil? || ary.is_a?(Array)
        @data = build_from(ary)
      end

      # insert an element list into heap, it will automatic adjust
      def insert(*elements)
        @mutex.synchronize do
          elements.each do |x|
            data.push(x)
            adjust(:bottom_up)
          end
        end
      end

      # take the heap element and adjust heap
      def shift
        @mutex.synchronize do
          return if data.empty?
          e = data.first
          data[0] = data.last
          data.pop
          adjust(:top_down)
          e
        end
      end

      # return the the top element of the heap
      def top
        data.first
      end

      private
      def build_from(ary)
        return [] if ary.nil? || ary.empty?

        heap = BinaryHeap.new(&(@cmp))
        heap.insert *ary
        heap.data
      end

      def swap(i, j)
        data[i], data[j] = data[j], data[i]
      end

      def compare(i, j)
        @cmp.call(data[i], data[j]) >= 0
      end

      def p_idx(child_idx)
        return nil if child_idx == 0
        child_idx%2 == 0 ? (child_idx-2)/2 : child_idx/2
      end

      def l_idx(parent_idx)
        (parent_idx << 1) + 1
      end

      def r_idx(parent_idx)
        (parent_idx << 1) + 2
      end

      def lchid(parent_idx)
        data[l_idx(parent_idx)]
      end

      def rchild(parent_idx)
        data[r_idx(parent_idx)]
      end

      def good?(idx)
        if !lchid(idx).nil?
          return false unless compare(idx, l_idx(idx))
        end

        if !rchild(idx).nil?
          return false unless compare(idx, r_idx(idx))
        end

        true
      end

      # make the heap in good shape
      def adjust(direction = :top_down)
        return if data.size < 2

        case direction
        when :top_down
          parent_idx = 0
          until good?(parent_idx)
            child_idx = find_child_idx(parent_idx)
            swap(parent_idx, child_idx)
            parent_idx = child_idx
          end
        when :bottom_up
          child_idx = data.size - 1
          parent_idx = p_idx(child_idx)
          until child_idx == 0 || compare(parent_idx, child_idx)
            swap(parent_idx, child_idx)
            child_idx = parent_idx
            parent_idx = p_idx(child_idx)
          end
        else
          raise "invalid direction type"
        end
      end

      def find_child_idx(parent_idx)
        l = lchid(parent_idx)
        r = rchild(parent_idx)

        return if l.nil? && r.nil?

        l_idx_ = l_idx(parent_idx)
        r_idx_ = r_idx(parent_idx)

        return r_idx_ if l.nil?
        return l_idx_ if r.nil?

        compare(l_idx_, r_idx_) ? l_idx_ : r_idx_
      end
    end
  end
end
