module Scotty
  export maptask

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

  function worker()
    try
      task = readobj()
      try
        task()
      catch exc
        writeint(0)
        writeint(2) # OOB 2: task error (fatal)
        writeobj(exc)
        rethrow(exc)
      end
    catch exc
      writeint(0)
      writeint(1) # OOB 1: internal error (fatal)
      writeobj(exc)
      rethrow(exc)
    end
    writeint(0)
    writeint(0) # OOB 0: done
  end
end
