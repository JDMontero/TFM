/home/jmontero/flume/bin/flume-ng agent -f /home/jmontero/TFM/Flume/test_identity.conf -Xmx1g --name Agent1 -Dflume.root.logger=INFO,console &
/home/jmontero/flume/bin/flume-ng agent -f /home/jmontero/TFM/Flume/test_transaction.conf -Xmx1g --name Agent2 -Dflume.root.logger=INFO,console &
/home/jmontero/flume/bin/flume-ng agent -f /home/jmontero/TFM/Flume/train_identity.conf -Xmx1g --name Agent3 -Dflume.root.logger=INFO,console &
/home/jmontero/flume/bin/flume-ng agent -f /home/jmontero/TFM/Flume/train_transaction.conf -Xmx1g --name Agent4 -Dflume.root.logger=INFO,console 
