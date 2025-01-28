import { Await, createFileRoute } from '@tanstack/react-router'
import { use, useEffect, useState } from 'react'

export const Route = createFileRoute('/stream')({
  component: Home,
  loader() {
    return {
      promise: new Promise<string>((resolve) =>
        setTimeout(() => resolve('promise-data'), 150),
      ),
      stream: new ReadableStream({
        async start(controller) {
          for (let i = 0; i < 5; i++) {
            await new Promise((resolve) => setTimeout(resolve, 200))
            controller.enqueue(`stream-data-${i} `)
          }
          controller.close()
        },
      }),
    }
  },
})

interface StreamData {
  accumulatedData: Array<string>
  subscribe: (cb: (accumulatedData: Array<string>) => void) => () => void
  initialValuePromise: Promise<string>
  finishedPromise: Promise<Array<string>>
}
const streamDataMap = new Map<ReadableStream, StreamData>()

function getStreamData(stream: ReadableStream): StreamData {
  let streamData = streamDataMap.get(stream)
  if (streamData) return streamData

  const subscriptions = new Set<(accumulatedData: Array<string>) => void>()
  let resolveFirstChunk!: (value: string) => void
  let resolveFinished!: (value: Array<string>) => void
  streamData = {
    accumulatedData: [],
    subscribe(callback) {
      subscriptions.add(callback)
      return () => subscriptions.delete(callback)
    },
    initialValuePromise: new Promise((resolve) => {
      resolveFirstChunk = resolve
    }),
    finishedPromise: new Promise((resolve) => {
      resolveFinished = resolve
    }),
  }

  streamDataMap.set(stream, streamData)
  ;(async function receiveData() {
    const reader = stream.pipeThrough(new TextDecoderStream()).getReader()

    let chunk
    while (!(chunk = await reader.read()).done) {
      const value = chunk.value
      streamData.accumulatedData.push(value)
      resolveFirstChunk(value)
      for (const subscription of subscriptions) {
        subscription(streamData.accumulatedData)
      }
    }
    resolveFinished(streamData.accumulatedData)
  })()

  return streamData
}

function Home() {
  const { promise, stream } = Route.useLoaderData()
  const [streamedValues, setStreamedValues] = useState<Array<string>>([])

  const streamData = getStreamData(stream)
  use(streamData.initialValuePromise)

  useEffect(() => {
    return streamData.subscribe(setStreamedValues)
  }, [])

  return (
    <>
      <div className="p-2" data-testid="promise-data">
        <div data-testid="stream-data">
          {streamedValues.map((d) => (
            <div key={d}>{d}</div>
          ))}
        </div>
      </div>
      <Await
        promise={streamData.finishedPromise}
        children={() => 'stream finished'}
      />
      <Await promise={promise} children={(promiseData) => promiseData} />
    </>
  )
}
