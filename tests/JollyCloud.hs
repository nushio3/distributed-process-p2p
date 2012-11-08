module Main where

import qualified Control.Distributed.Backend.P2P as P2P
import           Control.Distributed.Process as DP
import           Control.Distributed.Process.Node as DPN

import System.Environment (getArgs)

import Control.Monad
import Control.Monad.Trans (liftIO)
import Control.Concurrent (threadDelay)

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["-h"] -> putStrLn "Usage: jollycloud addr port [<seed>..]"
    host:port:seeds -> P2P.bootstrap host port (map P2P.makeNodeId seeds) mainProcess

mainProcess :: Process ()
mainProcess = do
    spawnLocal logger

    forever $ do
        liftIO $ threadDelay (10 * 1000000)
        P2P.getPeers >>= liftIO . print

logger :: Process ()
logger = do
    unregister "logger"
    getSelfPid >>= register "logger"
    forever $ do
        (time, pid, msg) <- expect :: Process (String, ProcessId, String)
        liftIO $ putStrLn $ time ++ " " ++ show pid ++ " " ++ msg
        return ()
