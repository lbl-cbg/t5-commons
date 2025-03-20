import '@jbrowse/react-linear-genome-view/esm/workerPolyfill'
import { initializeWorker } from '@jbrowse/product-core'
import { enableStaticRendering } from 'mobx-react'
import corePlugins from '@jbrowse/react-linear-genome-view/esm/corePlugins'
import CustomTrackPlugin from './custom-track-plugin'

enableStaticRendering(true)

initializeWorker([...corePlugins, CustomTrackPlugin], {
  fetchESM: url => import(url),
})
export default function doNothing() {}