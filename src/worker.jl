readint(input) = ntoh(read(input, Int32))
readobj(input) = deserialize(IOBuffer(uint8(readbytes(input, readint(input)))))

task = readobj(STDIN)
while !eof(STDIN)
  buf = IOBuffer()
  serialize(buf, task(readobj(STDIN)))
  arr = takebuf_array(buf)::Vector{Uint8}
  write(STDOUT, hton(int32(length(arr))))
  write(STDOUT, arr)
end
