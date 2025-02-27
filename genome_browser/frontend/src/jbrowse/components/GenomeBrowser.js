

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
//import {assembly} from "../assemblies/NC_003899_1";
//import {tracks} from "../tracks/NC_003899_1";
//import {defaultSession} from "../defaultSessions/NC_003899_1";
import "./genome-browser.css";


export default function GenomeBrowser(props) {

    const [viewState, setViewState] = useState();

    useEffect(() => {
      console.log("tid:", props.taxonId);

      async function fetchData() {
        let data = await getData(props.taxonId);
        if(!data.assembly)
          return;
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
          "assembly":data.assembly.assembly,
          "tracks":data.tracks.tracks,
          "defaultSession":data.defaultSession.defaultSession,
        });  

        setViewState(state);

        setTimeout(()=>{
          //highlight track labels
          let labels = ["YP_913811_1","YP_913812_1","YP_913813_1","NP_740652_1",
                        "YP_009724389_1_proteins","YP_009725295_1_proteins"];
          let trackLabels = document.querySelectorAll('div[class*="trackLabel"]');
          for(let e of trackLabels)
          {
            for(let label of labels)
            {
              if(e.innerHTML.includes(label))
              {
                e.classList.add("table-row-highlight"); 
              }
            } 
          } 
        }
        ,500);
        



      }

      fetchData();

      /*
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
      */
    }, [props.taxonId])


    const getData = async(taxonId) => {
      let data = {"assembly":null,
                  "tracks":null,
                  "defaultSession":null
                };

      if(taxonId==="11021")//EEEV
      {
        data.assembly = await import("../assemblies/NC_003899_1");
        data.tracks = await import("../tracks/NC_003899_1");
        data.defaultSession = await import("../defaultSessions/NC_003899_1");
      }
      else if(taxonId==="11036")//VEEV
      {}
      else if(taxonId==="37124")//CHIKV
      {}
      else if(taxonId==="2697049")//COVID2
      {
        data.assembly = await import("../assemblies/NC_045512_2");
        data.tracks = await import("../tracks/NC_045512_2");
        data.defaultSession = await import("../defaultSessions/NC_045512_2");
      }

      return data;
    }


    return (
        <div>
            {viewState &&
            <JBrowseLinearGenomeView viewState={viewState} height="300px"/>
            }
        </div>
    );

}