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
      check_result_code
      value
    end

    alias :[]= :put
    
    def get(key)
      cmd = [0xc8,0x30].pack('C2') + [key.length].pack('N') + key
      @socket.write(cmd)
      check_result_code
      len = @socket.read(4).unpack('N').first
      @socket.read(len)
    end
    
    alias :[] :get
    
    def out(key)
      cmd = [0xc8,0x20].pack('C2') + [key.length].pack('N') + key
      @socket.write(cmd)
      check_result_code
    end
    
    alias :delete :out
    
    def mget(keys)
      cmd = [0xc8,0x31].pack('C2') + [keys.length].pack('N') + keys.map {|key| [key.length].pack('N') + key}.join
      @socket.write(cmd)
      check_result_code
      result = {}
      num = @socket.read(4).unpack('N').first
      num.times do 
        key_len = @socket.read(4).unpack('N').first
        val_len = @socket.read(4).unpack('N').first
        key = @socket.read(key_len)
        val = @socket.read(val_len)
        result[key] = val
      end
      result
    end
    
    def vsiz(key)
      cmd = [0xc8,0x38].pack('C2') + [key.length].pack('N') + key
      @socket.write(cmd)
      check_result_code
      @socket.read(4).unpack('N').first
    end
    
    def iterinit
      cmd = [0xc8,0x50].pack('C2')
      @socket.write(cmd)
      check_result_code
    end
    
    def iternext
      cmd = [0xc8,0x51].pack('C2')
      @socket.write(cmd)
      check_result_code
      len = @socket.read(4).unpack('N').first
      @socket.read(len)
    end
    
    def each_key
      iterinit
      yield(iternext) while true
      rescue
    end
    
    def fwmkeys(prefix, max) # max should allow negative values, but it does not
      cmd = [0xc8,0x58].pack('C2') + [prefix.length,max].pack('N2') + prefix
      @socket.write(cmd)
      check_result_code
      keys = []
      num = @socket.read(4).unpack('N').first
      num.times do 
        key_len = @socket.read(4).unpack('N').first
        keys << @socket.read(key_len)
      end
      keys
    end
    
    def sync
      cmd = [0xc8,0x70].pack('C2')
      @socket.write(cmd)
      check_result_code
    end

    def vanish
      cmd = [0xc8,0x71].pack('C2')
      @socket.write(cmd)
      check_result_code
    end
    
    def copy(path)
      cmd = [0xc8,0x72].pack('C2') + [path.length].pack('N') + path
      @socket.write(cmd)
      check_result_code
    end
    
    def restore(path)
      cmd = [0xc8,0x73].pack('C2') + [path.length].pack('N') + path
      @socket.write(cmd)
      check_result_code
    end
    
    def setmst(host, port=1978)
      cmd = [0xc8,0x78].pack('C2') + [host.length, port].pack('N2') + host
      @socket.write(cmd)
      check_result_code
    end
    
    alias :master= :setmst
    
    def rnum
      cmd = [0xc8,0x80].pack('C2')
      @socket.write(cmd)
      check_result_code
      ns = @socket.read(8).unpack('N2')
      ns[0] * (2**32) + ns[1]
    end
    
    alias :count :rnum
    
    def size
      cmd = [0xc8,0x81].pack('C2')
      @socket.write(cmd)
      check_result_code
      ns = @socket.read(8).unpack('N2')
      ns[0] * (2**32) + ns[1]
    end
    
    def stat
      cmd = [0xc8,0x88].pack('C2')
      @socket.write(cmd)
      check_result_code
      len = @socket.read(4).unpack('N').first
      @socket.read(len)
    end
    
    

    private
    
    def check_result_code
      unless (code = @socket.read(1).unpack('C').first) == 0
        raise "TokyoTyrant error: #{code}"
      end
    end

    
  end
  
end