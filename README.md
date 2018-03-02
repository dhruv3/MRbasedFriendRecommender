# MRbasedFriendRecommender

MapReduce program in Hadoop that implements a simple “People You Might Know” social network friendship recommendation algorithm. The key idea is that if two people have a lot of mutual friends, then the system should recommend that they connect with each other.

Suppose the map input is line 0 1,2,3,4. Where 0 is the User and its friends are 1,2,3 and 4. Our map output will be something like <0,”1,SPECIAL_LIMITER”> and <1,”2,0”> The first value shows that User 1 cannot be
suggested 0 as both are already connected and to recognize this we are using
SPECIAL_LIMITER. In the second value User 1 can be suggested User 2 and their
common friend User 0 is also recorded.

These values reach reducer which ignores input containing SPECIAL_LIMITER and acts
on others. We sum over the inputs and sort to get list of top 10 recommended users.

[LiveJournal](https://snap.stanford.edu/data/soc-LiveJournal1.html) dataset was used to test our implementation.
