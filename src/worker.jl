include("scotty.jl")
using Scotty
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
