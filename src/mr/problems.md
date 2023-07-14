## Debug
To debug, you can either use dlv or print log. All the log start with `// log.Printf...` in the code.
Some of them are annotated, so you need to uncomment them to see them.

## Problems
### 1. The crashing test sometimes fails (fixed)
Initially, crashing test sometimes failed and after checking it was found that all maps outputs are correct, yet the final output always ignores something.

After checking, I found that it was because the retry wait time was too long (30s), 
which caused the script's timeout to be triggered even before the reassigned reduce task could finish.

This problem does not exist in other tasks, but in the crashing test, delays were triggered frequently, which amplified the problem.

### 2. Job count failed in WSL (bug)
It seems that in WSL, sometimes it will show duplicate files in one directory, which causes the job count to fail.
I have no idea why this happens, but it is not a problem in the code. 
You might find the related problem in [here](https://github.com/files-community/Files/issues/2943).

checklist:
- [x] 1. Check if the file is duplicated in the directory