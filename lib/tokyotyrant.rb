require 'socket'

module TokyoTyrant
  
  class Database
    
    def initialize(host,port=1978)
      @socket = TCPSocket.open(host, port)
    end

    def put(key, value, mode = nil)
      mod = 0x10
      case mode
      when :keep
        mod = 0x11
      when :cat
        mod = 0x12
      end
      cmd = [0xc8,mod].pack('C2') + [key.length, value.length].pack('N2') + key + value
      @socket.write(cmd)
      unless (code = @socket.read(1).unpack('C').first) == 0
        raise "TokyoTyrant error: #{code}"
      end
      value
    end

    alias :[]= :put
    
    def get(key)
      cmd = [0xc8,0x30].pack('C2') + [key.length].pack('N') + key
      @socket.write(cmd)
      unless (code = @socket.read(1).unpack('C').first) == 0
        raise "TokyoTyrant error: #{code}"
      end
      len = @socket.read(4).unpack('N').first
      @socket.read(len)
    end
    
    alias :[] :get
    

    
  end
  
end