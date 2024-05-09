

import Plugin from '@jbrowse/core/Plugin';

export default class CustomTrackPlugin extends Plugin {
    name = 'CustomTrackPlugin'
  
    install(pluginManager) {
  
      pluginManager.jexl.addFunction('colorFeature', feature => {
        return '#34eb74';
      });
  
      pluginManager.addToExtensionPoint(
        'Core-replaceWidget',
        (DefaultWidget, { model }) => {
          // replace widget for this particular track ID
          return  function NewWidget(props) {
                // this new widget adds a custom panel above the old DefaultWidget,
                // but you can replace it with any contents that you want
                if(model.trackId === 'YP_913811_1')
                {
                return (
                  <div>
                    <div style={{height:"200px",fontSize:"20px"}}>Custom content here....
                        hello
                    </div>
                    <DefaultWidget {...props} />
                  </div>
                )
                }
                else
                  return <DefaultWidget {...props} />
              }
        },
      );
  
      pluginManager.addToExtensionPoint(
        'Core-extendPluggableElement',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (pluggableElement) => {
          if (pluggableElement.name === 'LinearGenomeView') { console.log("AAA");
            const { stateModel } = pluggableElement
            const newStateModel = stateModel.extend(self => {  console.log("self:",self);
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