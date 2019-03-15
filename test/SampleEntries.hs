module SampleEntries where
import Protolude
import qualified Data.Sequence as Seq

import Raft
import Raft.Log

import RaftTestT
import TestUtils

--------------------------------------------------------------------------------
-- Sample entries
--------------------------------------------------------------------------------

genEntries :: Integer -> Integer -> Entries StoreCmd
genEntries numTerms numEntriesPerTerm =
  Seq.fromList $ fmap gen (zip indexes terms)
 where
  indexes = [1 .. numTerms * numEntriesPerTerm]
  terms = concatMap (replicate (fromInteger numEntriesPerTerm)) [1 .. numTerms]
  gen (i, t) = Entry (Index (fromInteger i))
                     (Term (fromInteger t))
                     NoValue
                     (LeaderIssuer (LeaderId node0))
                     genesisHash


entries :: Entries StoreCmd
entries = genEntries 4 3  -- 4 terms, each with 3 entries

entriesMutated :: Entries StoreCmd
entriesMutated = fmap
  (\e -> if entryIndex e == Index 12
    then e { entryIssuer = LeaderIssuer (LeaderId node1)
           , entryValue  = EntryValue $ Set "x" 2
           }
    else e
  )
  entries

