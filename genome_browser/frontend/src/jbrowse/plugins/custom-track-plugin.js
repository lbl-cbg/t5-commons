

import Plugin from '@jbrowse/core/Plugin';


function scrollToTargetRow(event,t){
  event.preventDefault();

  //hide feature detail modal
  const backfrops = document.getElementsByClassName("MuiBackdrop-root");
  backfrops[0].click();

  //highlight target table row
  window.speciesSetTargetRow(t);

  /*
  const cells = document.getElementsByClassName("MuiTableCell-root");
  for (let i = 0; i < cells.length; i++)
  { 
    let cell = cells.item(i);
    cell.parentElement.classList.remove("table-row-highlight");
  } 
  for (let i = 0; i < cells.length; i++)
  { 
    let cell = cells.item(i);
    if(cell.innerHTML.includes(t))
    {
      //hide feature detail modal
      const backfrops = document.getElementsByClassName("MuiBackdrop-root");
      backfrops[0].click();

      cell.parentElement.classList.add("table-row-highlight");
      const y = cell.parentElement.getBoundingClientRect().top + window.scrollY;
      window.scroll({
        top: y,
        behavior: 'smooth'
      });
      break;
    }
  }
  */
  
}

export default class CustomTrackPlugin extends Plugin {
    name = 'CustomTrackPlugin'
  
    install(pluginManager) {
  
      pluginManager.jexl.addFunction('colorFeature', (feature) => {
        
        /*console.log("F:",feature); 
        console.log("TR:",window.targetsTableRows); 
        let targetFound = window.targetsTableRows.find(item=>{
          return item.dataid ==feature.data.id;
        })
        */

        let targetid = feature.data.targetid;
        if(targetid)
          return '#0f9cd0'; //'#34eb74'

        let type = feature.data.type;  
        if (type === 'gene') {
          return "#1b8f13"; //green
        }
        else if (type.includes('protein') || type.includes('mat_peptide')) {
          return '#ac662b'; //golden brown
        } 
        else if (type === 'CDS') {
          return '#ff1312' //red
        } 
        else {
          return "#131314"; //black
        }

      });
  
      pluginManager.addToExtensionPoint(
        'Core-replaceWidget',
        (DefaultWidget, { model }) => {
          // replace widget for this particular track ID
          return  function NewWidget(props) {
                // this new widget adds a custom panel above the old DefaultWidget,
                // but you can replace it with any contents that you want
                if(model.featureData && model.featureData.targetid)
                {
                  return (
                    <div>
                      <div style={{height:"50px",display:"flex",alignItems:"center",gap:"0.5em"}}>
                        <svg className="MuiSvgIcon-root MuiSvgIcon-fontSizeMedium css-1wmkh38" 
                            focusable="false" 
                            aria-hidden="true" 
                            viewBox="0 0 24 24" data-testid="AdsClickIcon"
                            style={{width: "2em",
                                    height: "2em",
                                    display: "inline-block",
                                    fill: "#0f9cd0"}}
                            >
                              <path d="M11.71 17.99C8.53 17.84 6 15.22 6 12c0-3.31 2.69-6 6-6 3.22 0 5.84 2.53 5.99 5.71l-2.1-.63C15.48 9.31 13.89 8 12 8c-2.21 0-4 1.79-4 4 0 1.89 1.31 3.48 3.08 3.89zM22 12c0 .3-.01.6-.04.9l-1.97-.59c.01-.1.01-.21.01-.31 0-4.42-3.58-8-8-8s-8 3.58-8 8 3.58 8 8 8c.1 0 .21 0 .31-.01l.59 1.97c-.3.03-.6.04-.9.04-5.52 0-10-4.48-10-10S6.48 2 12 2s10 4.48 10 10m-3.77 4.26L22 15l-10-3 3 10 1.26-3.77 4.27 4.27 1.98-1.98z"></path>
                        </svg>
                        <a style={{cursor:"pointer",
                                    textDecoration:"underline",
                                    color:"#0f9cd0"}} 
                            onClick={(event) => scrollToTargetRow(event,model.featureData.targetid)}
                            title={"Target ID: "+model.featureData.targetid}>
                            {model.featureData.targetid}
                        </a>
                      </div>

                      <DefaultWidget {...props} />
                    </div>
                  )
                }
                /*
                else if(model.trackId === 'YP_913811_1')
                {
                  return (
                    <div>
                      <div style={{height:"200px",fontSize:"20px"}}>Custom content here....
                          Custom content....
                      </div>
                      <DefaultWidget {...props} />
                    </div>
                  )
                }
                */
                else
                  return <DefaultWidget {...props} />
              }
        },
      );
  
      pluginManager.addToExtensionPoint(
        'Core-extendPluggableElement', 
        (pluggableElement) => {
          if (pluggableElement.name === 'LinearGenomeView') { 
            const { stateModel } = pluggableElement
            const newStateModel = stateModel.extend(self => { 
              const superRubberBandMenuItems = self.rubberBandMenuItems
              return {
                views: {
                  rubberBandMenuItems() {
                    return [
                      ...superRubberBandMenuItems(),
                      {
                        label: 'Console log selected region',
                        onClick: () => {
                          const { leftOffset, rightOffset } = self
                          const selectedRegions = self.getSelectedRegions(
                            leftOffset,
                            rightOffset,
                          )
                          // console log the list of potentially multiple
                          // regions that were selected
                          console.log(selectedRegions)
                        },
                      },
                    ]
                  },
                },
              }
            })
  
            pluggableElement.stateModel = newStateModel
          }
          return pluggableElement
        },
      )
      
    }
  
    configure() {}
}