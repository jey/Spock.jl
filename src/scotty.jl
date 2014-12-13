module Scotty
  export readint, readobj, writeobj, maptask

  readint() = ntoh(read(STDIN, Int32))
  readobj() = deserialize(IOBuffer(uint8(readbytes(STDIN, readint()))))

  function writeobj(obj)
    buf = IOBuffer()
    serialize(buf, obj)
    arr = takebuf_array(buf)
    write(hton(int32(length(arr))))
    write(arr)
  end

  function maptask(f)
    function task()
      while !eof(STDIN)
        writeobj(f(readobj()))
      end
    end
  end
end
