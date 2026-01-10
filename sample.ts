import { DataplyAPI, type DataplyOptions, SerializeStrategyAsync, PageManagerFactory, Transaction } from 'dataply'

class DocumentDataplyAPI extends DataplyAPI {
  declare runWithDefault

  protected readonly pmf: PageManagerFactory

  constructor(file: string, options: DataplyOptions) {
    super(file, options)
    this.pmf = new PageManagerFactory()
    this.hook.onceAfter('init', async (tx, isNewlyCreated) => {
      if (isNewlyCreated) {
        console.log('database created!')
        const pk = await this.insertAsOverflow(JSON.stringify({ nickname: 'test', msg: 'test' }), false, tx)
        console.log('database first initialed!', pk)
      }
      console.log('init done!')
      return tx
    })
  }

}

class DocumentDataply {
  protected readonly api: DocumentDataplyAPI

  constructor(file: string, options?: DataplyOptions) {
    this.api = new DocumentDataplyAPI(file, options ?? {})
  }

  async init() {
    await this.api.init()
  }

  async insert(data: string, tx?: Transaction) {
    return this.api.runWithDefault(async (tx) => {
      return this.api.insert(data, true, tx)
    }, tx)
  }

  async select(pk: number, tx?: Transaction) {
    return this.api.runWithDefault(async (tx) => {
      return this.api.select(pk, false, tx)
    }, tx)
  }

  async close() {
    await this.api.close()
  }
}


const a = new DocumentDataply('test.dataply')


async function main() {
  await a.init()

  const pk = await a.insert(JSON.stringify({ nickname: 'a', msg: 'a' }))

  const data = await a.select(pk)

  console.log(pk, data)

  await a.close()
}

main()
