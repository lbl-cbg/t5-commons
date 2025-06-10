

import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    createViewState,
    JBrowseLinearGenomeView,
  } from '@jbrowse/react-linear-genome-view';
import CustomTrackPlugin from "../plugins/custom-track-plugin"; 
import "./genome-browser.css"; 
//import makeWorkerInstance from '@jbrowse/react-linear-genome-view/esm/makeWorkerInstance';
/*import ViewType from '@jbrowse/core/pluggableElementTypes/ViewType';
import PluginManager from '@jbrowse/core/PluginManager';
*/



export default function GenomeBrowser(props) {

    const [viewState, setViewState] = useState();

    useEffect(() => {
      console.log("tid:", props.taxonId, props.targetsTableRows);

      async function fetchData() {
        let data = await getData(props.taxonId, props.targetsTableRows);
        if(!data.assembly)
          return;
        const state = createViewState({
          "plugins":[CustomTrackPlugin], 
          "assembly":data.assembly.assembly,
          "tracks":data.tracks.tracks,
          "defaultSession":data.defaultSession.defaultSession,
          "configuration": {
            "theme": {
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
              }
            },
            "rpc": {
              "defaultDriver": 'WebWorkerRpcDriver',
            },
          },
          makeWorkerInstance: () =>
            new Worker(new URL('../plugins/rpcWorker', import.meta.url)),
          
        });  

        setViewState(state);

        setTimeout(()=>{
          //highlight track labels
          let labels = [];
          let targetsDict = {};
          for(let targetRow of props.targetsTableRows)
          {
            if(!(targetRow.targetid in targetsDict))
            { 
              labels.push(targetRow.originaltargetid);
              targetsDict[targetRow.targetid] = true;
            }
              
          }

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


    const getData = async(taxonId, targetsTableRows) => {
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
              <div>
                <div id="legend-container" >
                  <div className="legend-item target">
                    Target <div className="legend-icon"></div>
                  </div>
                  <div className="legend-item gene">
                    Gene <div className="legend-icon"></div>
                  </div>
                  <div className="legend-item protein">
                  Protein <div className="legend-icon"></div>
                  </div>
                  <div className="legend-item cds">
                    CDS <div className="legend-icon"></div>
                  </div>
                  <div className="legend-item other">
                    Other <div className="legend-icon"></div>
                  </div>
                </div>
                <JBrowseLinearGenomeView viewState={viewState} height="300px"/>
              </div>
            }
        </div>
    );

}