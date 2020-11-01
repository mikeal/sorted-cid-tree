import * as main from '../index.js'
import * as codec from '@ipld/dag-cbor'
import raw from 'multiformats/codecs/raw'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { encode as _encode, decode as _decode } from 'multiformats/block'
import { deepStrictEqual as same, ok } from 'assert'

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

const create = async () => {
  const { put, get } = mock()
  const blocks = await Promise.all(fixtures)
  let last
  const structure = []
  for await (const block of main.fromBlocks(blocks)) {
    await put(block)
    last = block
    structure.push(block)
  }
  return { put, get, structure, blocks, root: last.cid }
}

export default async test => {
  test('basic map (512 entries)', async test => {
    const { get, structure, blocks, root } = await create()
    /*
    console.log({
      length: structure.map(() => 1).reduce((x,y) => x + y),
      size: structure.map(b => b.bytes.byteLength).reduce((x,y) => x + y)
    })
    */
    for (const block of blocks) {
      const depth = await main.has(block.cid, root, get, { fullDepth: true })
      same(depth, 3)
    }
    const rand = await encode({ value: Math.random() })
    try {
      await main.has(rand.cid, root, get)
    } catch (e) {
      if (!e.message.includes('Not found')) throw e
    }
  })
  test('range query (full)', async test => {
    const { get, blocks, root } = await create()
    const expecting = blocks.map(b => b.cid).sort(({ bytes: a }, { bytes: b }) => main.compare(a, b))
    for await (const cid of main.range({ get, root })) {
      const expected = expecting.shift()
      if (cid.toString() !== expected.toString()) throw new Error('Not expecting')
    }
    if (expecting.length) throw new Error('Did not emit all blocks')

    for await (const test of main.range({ get, root, start: main.hardStop })) {
      throw new Error('should not see any entries')
    }
  })
  test('range query (narrowing)', async test => {
    const { get, blocks, root } = await create()
    const expecting = new Set(blocks.map(b => b.cid.toString()))

    let sorted = blocks.map(b => b.cid).sort(({ bytes: a }, { bytes: b }) => main.compare(a, b))

    while (sorted.length) {
      sorted = sorted.slice(1, sorted.length -2)
      const start = sorted[0]
      const end = sorted[sorted.length -1]
      const expecting = [...sorted]
      if (!start || !end) break
      for await (const cid of main.range({ get, root, start, end })) {
        const expected = expecting.shift()
        if (cid.toString() !== expected.toString()) throw new Error('Not expecting')
      }
      if (expecting.length) {
        throw new Error('Did not emit all blocks, missing ' + expecting.length)
      }
    }
  })
}
