import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { encode as _encode } from 'multiformats/block'

const encode = async opts => {
  const block = await _encode({ codec, hasher, ...opts })
  return block
}

const compare = (b1, b2) => {
  for (let i = 0; i < b1.byteLength; i++) {
    if (b2.byteLength === i) return 1
    const c1 = b1[i]
    const c2 = b2[i]
    if (c1 === c2) continue
    if (c1 > c2) return 1
    else return -1
  }
  if (b2.byteLength > b1.byteLength) return -1
  return 0
}

const chunker = async function * (arr) {
  if (!arr.length) throw new Error('Cannot chunk zero length array')
  arr = arr.sort(({ bytes: a }, { bytes: b }) => compare(a, b))
  let chunks = []
  for (const cid of arr) {
    const { bytes } = cid
    const i = bytes[bytes.byteLength - 1]
    chunks.push(cid)
    if (i === 0) {
      yield [chunks[0], await encode({ value: chunks })]
      chunks = []
    }
  }
  if (chunks.length) {
    yield [chunks[0], await encode({ value: chunks })]
  }
}

const fromBlocks = async function * (arr) {
  let branches = []
  for await (const [first, block] of chunker(arr.map(({ cid }) => cid))) {
    yield block
    branches.push([first, block])
  }

  while (branches.length !== 1) {
    const chunk = branches
    branches = []
    let parts = []
    for (const [first, { cid, bytes }] of chunk) {
      const i = bytes[bytes.byteLength - 1]
      parts.push([first, cid])
      if (i === 0) {
        const branchBlock = await encode({ value: parts })
        yield branchBlock
        branches.push([parts[0][0], branchBlock])
        parts = []
      }
    }
    if (parts.length) {
      const branchBlock = await encode({ value: parts })
      yield branchBlock
      branches.push([parts[0][0], branchBlock])
    }
  }
}

const has = async (cid, root, get, opts = {}) => {
  const { fullDepth } = opts
  let branch = root
  let _prev
  let depth = 0
  while (!branch.equals(cid)) {
    depth += 1
    if (_prev && _prev.equals(branch)) throw new Error(`Not found in tree “"${cid.toString()}"`)
    const { value: entries } = await get(branch)
    _prev = branch
    let isBranch
    for (const next of entries) {
      let key
      let value
      if (next.asCID === next) {
        key = next
        isBranch = false
      } else {
        key = next[0]
        value = next[1]
        isBranch = true
      }
      const comp = compare(key.bytes, cid.bytes)
      if (comp === 0) {
        if (!fullDepth || !isBranch) return depth
        branch = value
        break
      }
      if (comp < 0) {
        if (isBranch) branch = value
      } else {
        break
      }
    }
  }
}

const hardStop = new Uint8Array([...Array(512).keys()].map(() => 255))

const range = async function * ({ start, end, root, get }) {
  if (!start) start = new Uint8Array([0])
  if (!end) end = hardStop
  if (start.asCID === start) start = start.bytes
  if (end.asCID === end) end = end.bytes
  const { value: parent } = await get(root)
  if (Array.isArray(parent[0])) {
    // branch node
    for (let i = 0; i < parent.length; i++) {
      const [first, link] = parent[i]
      const next = parent[i + 1] ? parent[i + 1][0] : null

      const query = { start, end, root: link, get }

      let comp
      if (!next) {
        comp = compare(first.bytes, end)
        if (comp < 1) {
          yield * range(query)
        }
      } else {
        comp = compare(next.bytes, end)
        if (comp < 1) {
          comp = compare(first.bytes, end)
          if (comp > 0) continue
          yield * range(query)
        }
      }
    }
  } else {
    // leaf node
    for (const cid of parent) {
      let comp = compare(cid.bytes, end)
      if (comp > 0) return
      comp = compare(cid.bytes, start)
      if (comp > -1) {
        yield cid
      }
    }
  }
}

export { fromBlocks, has, range, compare, hardStop }
