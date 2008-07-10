require 'socket'
module TokyoTyrant
  
  class Database
    
    def initialize(host,port=1978)
      @socket = TCPSocket.open(host, port)
    end

    def put(key, value)
      cmd = [0xc8,0x10].pack('C2') + [key.length, value.length].pack('N2') + key + value
      @socket.write(cmd)
    end
    
    alias :[]= :put
    
  end
  
end