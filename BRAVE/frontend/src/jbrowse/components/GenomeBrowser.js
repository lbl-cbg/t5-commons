

import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    createViewState,
    JBrowseLinearGenomeView,
  } from '@jbrowse/react-linear-genome-view';
/*
import makeWorkerInstance from '@jbrowse/react-linear-genome-view/esm/makeWorkerInstance';
import ViewType from '@jbrowse/core/pluggableElementTypes/ViewType';
import PluginManager from '@jbrowse/core/PluginManager';
*/
import CustomTrackPlugin from "../plugins/custom-track-plugin";
import {assembly} from "../assemblies/NC_003899_1";
import {tracks} from "../tracks/NC_003899_1";
import {defaultSession} from "../defaultSessions/NC_003899_1";
import "./genome-browser.css";


export default function GenomeBrowser() {

    const [viewState, setViewState] = useState();

    

    useEffect(() => { 
        const state = createViewState({
          "configuration": {
            "theme" :{
              "logoPath": {
                "uri": ""
              },
              "palette": {
                //"primary": {
                //  "main": "#311b92"
                //},
                "secondary": {
                  "main": "#d3ebee"
                },
                //"tertiary": {
                //  "main": "#f57c00"
                //},
                //"quaternary": {
                //  "main": "#d50000"
                //}
              }}
        },
        "plugins":[CustomTrackPlugin], 
        assembly,
        tracks,
        defaultSession,
        });
       
        setViewState(state);
    }, [])




    return (
        <div>
            {viewState &&
            <JBrowseLinearGenomeView viewState={viewState} />
            }
        </div>
    );

}