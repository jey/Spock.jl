readint(input) = ntoh(read(input, Int32))
readobj2(input) = deserialize(IOBuffer(uint8(readbytes(input, readint(input)))))

function readobj(inp)
  obj = readobj2(inp)
  println(STDERR, "obj: $(obj)")
  obj
end

task = readobj(STDIN)
while !eof(STDIN)
  buf = IOBuffer()
  serialize(buf, task(readobj(STDIN)))
  arr = takebuf_array(buf)::Vector{Uint8}
  write(STDOUT, hton(int32(length(arr))))
  serialize(STDOUT, arr)
end
