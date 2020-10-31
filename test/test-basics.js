import * as main from '../index.js'
import * as codec from '@ipld/dag-cbor'
import raw from 'multiformats/codecs/raw'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { encode as _encode, decode as _decode } from 'multiformats/block'
import { deepStrictEqual as same } from 'assert'

const encode = opts => _encode({ codec, hasher, ...opts })
const decode = opts => _decode({ codec, hasher, ...opts })

const fixtures = [...Array(512).keys()].map(i => encode({ value: i }))
// const largeFixtures = [...Array(256 * 512).keys()].map(i => encode({ value: i }))

const mock = () => {
  const db = new Map()
  const get = cid => {
    const key = cid.toString()
    if (!db.has(key)) throw new Error('Missing block ' + key)
    return db.get(key)
  }
  const put = async block => {
    const key = block.cid.toString()
    return db.set(key, block)
  }
  return { get, put }
}

export default async test => {
  test('basic map (512 entries)', async test => {
    const { put, get } = mock()
    const blocks = await Promise.all(fixtures)
    let last
    for await (const block of main.fromBlocks(blocks)) {
      await put(block)
      last = block
    }
    for (const block of blocks) {
      const cid = await main.has(block.cid, last.cid, get)
      same(cid.equals(block.cid), true)
    }
  })
}
