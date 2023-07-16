
## Leader election
Watch this video: https://www.youtube.com/watch?v=9sT03jEwcaw to have a general idea of raft.
Then read RAFT 5.1, 5.2 and page 3.
This should be the easiest part of raft.
And remember a few details:

1. To generate a random election timeout in different goroutines, set proper seed for `rand`.
A common mistake is
```go
rand.Seed(time.Now().UnixNano())
randomDelay := rand.Int63n(rf.maxDelay-rf.minDelay) + rf.minDelay
time.Sleep(time.Duration(randomDelay) * time.*Millisecond)
```
Because the goroutines are running almost at the same time, the seed is the same.
One way is to use 'rf.me' as the seed.

2. Sending vote request in parallel and counter votes asynchronously.
If not, then under situation that some servers are having network problems, 
the leader will be elected very slowly (you have to wait for RPC timeout to determine peer voting or not).

## Log replication
![img.png](img.png)

Q: why a leader can only commit log entries from its current term?

A: In pic above, S1 is replaced by S5 as the leader in (b), and this election satisfies the 5.4.1 election restriction.
Then, S5 receive entry 3, after this, S5 crashes, and S1 becomes the leader again in (c).
If S1 can commit entry 3, then S5 can still be the leader in (d) according to 5.4.1 election restriction.
After S5 being elected, S5 begins to replicate entry 3 to S1, but S1 has already committed entry 2 at index 2.
Namely, S1 is trying to change some entry already committed. It causes inconsistency.

Q: why a leader cannot determine commitment using log entries from older terms?

A: At the very beginning, I don't think this question makes any sense, as we always use commitIndex to determine commitment.  

After thinking, I realize that it is exactly "leader cannot determine commitment using log entries from older terms" 
that makes 'commitIndex' field necessary.
