"""
lsh.py
Algorithms based on 'Mining of Massive Datasets'
"""
from collections import defaultdict
from unionfind import UnionFind
from whoosh.analysis import RegexTokenizer,IntraWordFilter,LowercaseFilter,StopFilter,ShingleFilter,MultiFilter


class Shingles:
    def shingle(self, s, k):
        """Generate k-length shingles of string s."""
        # k = min(len(s), k)
        # for i in range(len(s) - k + 1):
        #     yield s[i:i+k]
        rt = RegexTokenizer(r"\S+")
        # iwf_i = IntraWordFilter(splitwords=False,mergewords=True, mergenums=True)
        # iwf_q = IntraWordFilter(splitwords=False,mergewords=True, mergenums=False)
        # iwf = MultiFilter(index=iwf_i, query=iwf_q)
        iwf = IntraWordFilter()
        lcf = LowercaseFilter()
        stf = StopFilter()

        analyzer = rt | iwf | lcf | stf
        stream = analyzer(s)

        ngf = ShingleFilter(k,' ')

        return [token.text for token in ngf(stream)]


    def hshingle(self, s, k):
        """Generate k-length shingles then hash."""
        for s in self.shingle(s, k):
            yield hash(s)

    def jaccard(self, X, Y):
        """The Jaccard similarity between two sets."""
        x = set(X)
        y = set(Y)
        return float(len(x & y)) / len(x | y)

class Signature(object):
    """Signature Base class."""

    def __init__(self, dim):
        self.dim = dim
        self.hashes = self.hash_functions()

    def hash_functions(self):
        """Returns dim different hash functions"""
        pass

    def sign(self, object):
        """Return the signature for object s"""
        pass


class MinHashSignature(Signature):
    """Creates signatures for sets/tuples using minhash."""

    def hash_functions(self):
        """Return dim different hash functions"""
        def hash_factory(n):
            return lambda x: hash("salt" + unicode(n) + unicode(x) + "salt")
        return [ hash_factory(_) for _ in range(self.dim) ]

    def sign(self, s):
        """Returns minhash signature for set s"""
        sig = [ float("inf") ] * self.dim
        for hash_ix, hash_fn in enumerate(self.hashes):
            # minhashing; requires item is iterable:
            sig[hash_ix] = min(hash_fn(value) for value in s)
        return sig


class LSH(object):
    """Locality sensitive hashing. Uses a banding approach to hash
    similar signatures to the same buckets."""
    def __init__(self, length, threshold):
        self.length = length
        self.threshold = threshold
        self.bandwidth = self.get_bandwidth(length, threshold)

    def hash(self, sig):
        """Generate hashvals for this signature"""
        for band in zip(*(iter(sig),) * self.bandwidth):
            yield hash("salt" + unicode(band) + "tlas")

    def get_bandwidth(self, n, t):
        """Approximates the bandwidth (number of rows in each band)
        needed to get threshold.

        Threshold t = (1/b) ** (1/r) where
        b = #bands
        r = #rows per band
        n = b * r = #elements in signature
        """

        best = n, 1
        minerr = float("inf")
        for r in range(1, n + 1):
            try:
                b = 1. / (t ** r)
            except: # Divide by zero, your signature is huge
                return best
            err = abs(n - b * r)
            if err < minerr:
                best = r
                minerr = err
        return best

    def get_threshold(self):
        r = self.bandwidth
        b = self.length / r
        return (1. / b) ** (1. / r)

    def get_n_bands(self):
        return int(self.length / self.bandwidth)


class Cluster(object):
    """Clusters sets with Jaccard similarity above threshold with high
    probability.

    Algorithm based on Rajaraman, "Mining of Massive Datasets":
        1. Generate set signature
        2. Use LSH to map similar signatures to same buckets
        3. Use UnionFind to merge buckets containing same values
        """
    def __init__(self, width=10, threshold=0.5):
        self.width = width
        self.unionfind = UnionFind()
        self.signer = MinHashSignature(width)
        self.hasher = LSH(width, threshold)
        self.hashmaps = [defaultdict(list)
                for _ in range(self.hasher.get_n_bands())]

    def add_set(self, s, label=None):
        # A label for this set
        if not label:
            label = s
        # Add to unionfind structure
        self.unionfind[label]
        # Get signature
        sig = self.signer.sign(s)
        # Union labels with same LSH key in same band
        for band_idx, hshval in enumerate(self.hasher.hash(sig)):
             self.hashmaps[band_idx][hshval].append(label)
             self.unionfind.union(label, self.hashmaps[band_idx][hshval][0])

    def get_sets(self):
        return self.unionfind.sets()

def shingle(s, k):
    """Generate k-length shingles of string s"""
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]

def hshingle(s, k):
    """Generate k-length shingles then hash"""
    for s in shingle(s, k):
        yield hash(s)

def jaccard_sim(X, Y):
    """Jaccard similarity between two sets"""
    x = set(X)
    y = set(Y)
    return float(len(x & y)) / len(x | y)

def jaccard_dist(X, Y):
    """Jaccard distance between two sets"""
    return 1 - jaccard_sim(X, Y)
