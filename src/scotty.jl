module Scotty
  export readint, readobj, writeint, writeobj, maptask

  readint() = ntoh(read(STDIN, Int32))
  readobj(len) = deserialize(IOBuffer(uint8(readbytes(STDIN, len))))
  readobj() = readobj(readint())
  writeint(x) = write(hton(int32(x)))

  function writeobj(obj)
    buf = IOBuffer()
    serialize(buf, obj)
    arr = takebuf_array(buf)
    writeint(length(arr))
    write(arr)
  end

  function maptask(f)
    () -> begin
      while (len = readint()) != 0
        writeobj(f(readobj(len)))
      end
    end
  end
end
