readint(input=STDIN) = ntoh(read(input, Int32))
readobj(input=STDIN) = deserialize(IOBuffer(uint8(readbytes(input, readint(input)))))

task = readobj()
while !eof(STDIN)
  buf = IOBuffer()
  serialize(buf, task(readobj()))
  arr = takebuf_array(buf)::Vector{Uint8}
  write(hton(int32(length(arr))))
  write(arr)
end
