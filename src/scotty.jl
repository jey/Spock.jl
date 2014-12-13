module Scotty
  export maptask, reducetask

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

  function objseq(f)
    while (len = readint()) != 0
      f(readobj(len))
    end
  end

  function maptask(f)
    () -> begin
      objseq() do arg
        writeobj(f(arg))
      end
    end
  end

  function reducetask(f)
    () -> begin
      accum = nothing
      objseq() do arg
        if accum == nothing
          accum = arg
        else
          accum = f(arg, accum)
        end
      end
      writeobj(accum)
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
