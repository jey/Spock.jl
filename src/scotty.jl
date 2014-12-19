module Scotty
  export maptask, reducetask, pipetask, intask, outtask
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

  function intask()
    Task() do
      while (len = readint()) != 0
        produce(readobj(len))
      end
    end
  end

  function outtask(inp)
    map(writeobj, inp)
  end

  function maptask(f)
    (inp) -> begin
      Task() do
        map(arg -> produce(f(arg)), inp)
      end
    end
  end

  function reducetask(f)
    (inp) -> begin
      Task() do
        accum = nothing
        for arg in inp
          if accum == nothing
            accum = arg
          else
            accum = f(arg, accum)
          end
        end
        produce(accum)
      end
    end
  end

  function pipetask(t2, t1)
    (inp) -> t2(t1(inp))
  end

  function worker()
    try
      redirect_stdout(STDERR)
      close(redirect_stdin()[2])
      task = readobj()
      try
        outtask(task(intask()))
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
