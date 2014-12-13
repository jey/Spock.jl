include("scotty.jl")
using Scotty
try
  task = readobj()
  try
    task()
  catch exc
    write(int32(0))
    write(int16(2)) # OOB 2: task error (fatal)
    writeobj(exc)
    rethrow(exc)
  end
catch exc
  write(int32(0))
  write(int16(1)) # OOB 1: internal error (fatal)
  writeobj(exc)
  rethrow(exc)
end
write(int32(0))
write(int16(0)) # OOB 0: done
