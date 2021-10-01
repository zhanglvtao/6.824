cd /home/zhanglvtao/6.824/src/mrapps && go build -race -buildmode=plugin wc.go
cd /home/zhanglvtao/6.824/src/main
go build -race mrworker.go
#dlv exec ./mrworker  -- /home/zhanglvtao/6.824/src/mrapps/wc.so
./mrworker /home/zhanglvtao/6.824/src/mrapps/wc.so &
./mrworker /home/zhanglvtao/6.824/src/mrapps/wc.so &
./mrworker /home/zhanglvtao/6.824/src/mrapps/wc.so 
