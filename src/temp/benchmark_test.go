D:\HIT\coroprove\src\t_txn>go run aria\benchmark\main.go
Aria
TPCC Low Conflict:
core    tps     time    write_conflict  read_conflict
tops len 5
txn 4 begin r:r100019 w:r99999 r:r100012 r:r99991 w:r99985 r:r100009 r:r99980 w:r100003 r:r100004 r:r100025

txn 0 begin r:r99998 r:r100022 r:r99995 w:r100005 w:r100009 r:r99992 r:r100015 r:r100012 r:r100007 r:r100007

txn 1 begin r:r100009 r:r99996 r:r100011 r:r100009 r:r99985 r:r100001 w:r99991 r:r100001 r:r100002 r:r100007

txn 2 begin w:r99989 r:r100003 r:r99988 r:r100009 r:r100009 r:r100005 r:r100002 r:r100001 r:r99983 r:r100011

txn 3 begin r:r99992 r:r99992 w:r100014 r:r99996 r:r99997 w:r100017 r:r99997 r:r99989 r:r99993 r:r99980

txn 3 read r99992 ok
txn 3 read r99992 ok
txn 3 write r100014 ok
txn 3 read r99996 ok
txn 3 read r99997 ok
txn 3 write r100017 ok
txn 3 read r99997 ok
txn 3 read r99989 abort
txn 3 finished in this phase
txn 1 read r100009 ok
txn 1 read r99996 ok
txn 1 read r100011 ok
txn 1 read r100009 ok
txn 1 read r99985 ok
txn 1 read r100001 ok
txn 1 write r99991 ok
txn 1 read r100001 ok
txn 1 read r100002 ok
txn 1 read r100007 ok
txn 1 may be ok
txn 1 finished in this phase
txn 4 read r100019 ok
txn 4 write r99999 ok
txn 4 read r100012 ok
txn 4 read r99991 abort
txn 4 finished in this phase
txn 0 read r99998 ok
txn 0 read r100022 ok
txn 0 read r99995 ok
txn 0 write r100005 ok
txn 0 write r100009 ok
txn 0 read r99992 ok
txn 0 read r100015 ok
txn 0 read r100012 ok
txn 0 read r100007 ok
txn 0 read r100007 ok
txn 0 may be ok
txn 0 finished in this phase
txn 2 write r99989 ok
txn 2 read r100003 ok
txn 2 read r99988 ok
txn 2 read r100009 abort
txn 2 finished in this phase
============ phase 0 ok ============
txn 0 rm: [r100007: -1] [r99998: -1] [r100022: -1] [r99995: -1] [r99992: -1] [r100015: -1] [r100012: -1] , wm:[r100005: 0] [r100009: 0]
txn 0 done
txn 1 rm: [r100007: -1] [r100009: 0] [r99996: -1] [r100011: -1] [r99985: -1] [r100001: -1] [r100002: -1] , wm:[r99991: 1]
txn 1 done
txn 2 rm: [r100003: -1] [r99988: -1] [r100009: 0] , wm:[r99989: 2]
txn 2 done
txn 3 rm: [r99992: -1] [r99996: -1] [r99997: -1] [r99989: 2] , wm:[r100014: 3] [r100017: 3]
txn 3 done
txn 4 rm: [r100019: -1] [r100012: -1] [r99991: 1] , wm:[r99999: 4]
txn 4 done
1       0.9982179613310725      5.0089261s      0       0