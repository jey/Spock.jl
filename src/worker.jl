readint() = ntoh(read(STDIN, Int32))
readobj() = deserialize(IOBuffer(uint8(readbytes(STDIN, readint()))))

function writeobj(obj)
  buf = IOBuffer()
  serialize(buf, obj)
  arr = takebuf_array(buf)
  write(hton(int32(length(arr))))
  write(arr)
end

writeexc(exc) = writeobj(exc)

try
  task = readobj()
  while !eof(STDIN)
    arg = readobj()
    try
      result = task(arg)
    catch exc
      write(int32(0))
      write(int16(2)) # OOB 2: task error (fatal)
      writeexc(exc)
      rethrow(exc)
    end
    writeobj(result)
  end
catch exc
  write(int32(0))
  write(int16(1)) # OOB 1: internal error (fatal)
  writeexc(exc)
  rethrow(exc)
end
write(int32(0))
write(int16(0)) # OOB 0: done
