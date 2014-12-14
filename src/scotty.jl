module Scotty
  export maptask, reducetask
  const inf = STDIN
  const outf = STDOUT

  readint() = ntoh(read(inf, Int32))
  readobj(len) = deserialize(IOBuffer(uint8(readbytes(inf, len))))
  readobj() = readobj(readint())
  writeint(x) = write(outf, hton(int32(x)))

  function writeobj(obj)
    buf = IOBuffer()
    serialize(buf, obj)
    arr = takebuf_array(buf)
    writeint(length(arr))
    write(outf, arr)
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
      redirect_stdout(STDERR)
      close(redirect_stdin()[2])
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
