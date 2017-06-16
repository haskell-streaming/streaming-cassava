{- |
   Module      : Main
   Description : Round-trip property testing
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Main (main) where

import           Streaming
import           Streaming.Cassava
import qualified Streaming.Prelude as S

import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck

import Control.Monad.Except
import Data.Functor.Identity (Identity, runIdentity)

--------------------------------------------------------------------------------

main :: IO ()
main = undefined

encodeDecode :: (FromRecord a, ToRecord a, Eq a) => [a] -> Bool
encodeDecode as = either (const False) (as==) . streamList
                  . decode NoHeader . encode Nothing
                  . S.each $ as

streamList :: Stream (Of a) (Except e) () -> (Either e [a])
streamList = runExcept . S.toList_
