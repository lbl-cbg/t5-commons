


export const tracks = [
    {
        type: 'FeatureTrack',
        trackId: 'ORF1ab',
        name: 'ORF1ab',
        assemblyNames: ['NC_045512.2'],
        category: ['Genes'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/ORF1ab.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "ORF1ab-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)",
              "displayMode": "compact",  
            }
          }
        ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009724389_1_CDS',
      name: 'YP_009724389_1',
      assemblyNames: ['NC_045512.2'],
      category: ['CDS'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009724389_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009724389_1_CDS-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725297_1',
      name: 'nsp1',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725297_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725297_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725298_1',
      name: 'nsp2',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725298_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725298_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725299_1',
      name: 'nsp3',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725299_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725299_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725300_1',
      name: 'nsp4',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725300_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725300_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725301_1',
      name: 'nsp5',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725301_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725301_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ], 
      //"formatDetails":{"feature":{"hello":"newewewewewewew"}}
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725302_1',
      name: 'nsp6',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725302_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725302_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725303_1',
      name: 'nsp7',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725303_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725303_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725304_1',
      name: 'nsp8',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725304_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725304_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725305_1',
      name: 'nsp9',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725305_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725305_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725306_1',
      name: 'nsp10',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725306_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725306_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725307_1',
      name: 'nsp12',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725307_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725307_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725308_1',
      name: 'nsp13',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725308_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725308_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725309_1',
      name: 'nsp14',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725309_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725309_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725310_1',
      name: 'nsp15',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725310_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725310_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725311_1',
      name: 'nsp16',
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725311_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725311_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725295_1_CDS',
      name: 'YP_009725295_1',
      assemblyNames: ['NC_045512.2'],
      category: ['CDS'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725295_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725295_1_CDS-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742608_1',
      name: 'YP_009742608_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742608_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742608_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742609_1',
      name: 'YP_009742609_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742609_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742609_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742610_1',
      name: 'YP_009742610_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742610_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742610_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact", 
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742611_1',
      name: 'YP_009742611_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742611_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742611_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742612_1',
      name: 'YP_009742612_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742612_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742612_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742613_1',
      name: 'YP_009742613_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742613_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742613_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742614_1',
      name: 'YP_009742614_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742614_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742614_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742615_1',
      name: 'YP_009742615_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742615_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742615_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742616_1',
      name: 'YP_009742616_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742616_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742616_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009742617_1',
      name: 'YP_009742617_1', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009742617_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009742617_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_009725312_1',
      name: 'nsp11', 
      assemblyNames: ['NC_045512.2'],
      category: ['Proteins'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/YP_009725312_1.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "YP_009725312_1-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)",
            "displayMode": "compact",  
          }
        }
      ]
    },

























    {
        type: 'FeatureTrack',
        trackId: 'YP_009724390_1',
        name: 'YP_009724390_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724390_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724390_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724391_1',
        name: 'YP_009724391_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724391_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724391_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724392_1',
        name: 'YP_009724392_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724392_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724392_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724393_1',
        name: 'YP_009724393_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724393_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724393_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724394_1',
        name: 'YP_009724394_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724394_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724394_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724395_1',
        name: 'YP_009724395_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724395_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724395_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009725318_1',
        name: 'YP_009725318_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009725318_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009725318_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724396_1',
        name: 'YP_009724396_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724396_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724396_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009724397_2',
        name: 'YP_009724397_2',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009724397_2.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009724397_2-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
      },
      {
        type: 'FeatureTrack',
        trackId: 'YP_009725255_1',
        name: 'YP_009725255_1',
        assemblyNames: ['NC_045512.2'],
        category: ['Proteins'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/YP_009725255_1.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "YP_009725255_1-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
    },
    {
        type: 'FeatureTrack',
        trackId: 'stem_loop_features',
        name: 'stem_loop_features',
        assemblyNames: ['NC_045512.2'],
        category: ['Features'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/stem_loop_features.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "stem_loop_features-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
    },
    {
        type: 'FeatureTrack',
        trackId: '3_UTR_features',
        name: '3_UTR_features',
        assemblyNames: ['NC_045512.2'],
        category: ['Features'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_045512_2/3_UTR_features.gff3',
            }
        },
        "displays": [
          {
            "type": "LinearBasicDisplay",
            "displayId": "3_UTR_features-LinearBasicDisplay",
            "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" 
            }
          }
        ]
    },
    {
      type: 'FeatureTrack',
      trackId: '5_UTR_features',
      name: '5_UTR_features',
      assemblyNames: ['NC_045512.2'],
      category: ['Features'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_045512_2/5_UTR_features.gff3',
          }
      },
      "displays": [
        {
          "type": "LinearBasicDisplay",
          "displayId": "5_UTR_features-LinearBasicDisplay",
          "renderer": {
            "type": "SvgFeatureRenderer",
            "color1": "jexl:colorFeature(feature)" 
          }
        }
      ]
    },


















    /*
    {
        type: 'FeatureTrack',
        trackId: 'YP_913811_1',
        name: 'EEEV_MT',
        assemblyNames: ['NC_003899.1'],
        category: ['Genes'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_003899_1/YP_913811_1.gff3',
            }
        },
        "formatDetails": {
            "feature": "jexl:{Extra_links:'link to somewhere: '+'<a href=https://google.com/?q='+feature.name+' target=_blank>'+feature.name+'</a>',type:undefined }"
        },
        "displays": [
            {
            "type": "LinearBasicDisplay",
            "displayId": "YP_913811_1-LinearBasicDisplay",
            "renderer": {
                "type": "SvgFeatureRenderer",
                "color1": "jexl:colorFeature(feature)" //"#34eb74"
            }
            }
        ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_913812_1_EEEV_HEL',
      name: 'EEEV_HEL',
      assemblyNames: ['NC_003899.1'],
      category: ['Genes'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_003899_1/YP_913812_1_EEEV_HEL.gff3',
          }
      },
      "displays": [
          {
          "type": "LinearBasicDisplay",
          "displayId": "YP_913812_1_EEEV_HEL-LinearBasicDisplay",
          "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" //"#34eb74"
          }
          }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_913812_1_EEEV_HEL_PEPT',
      name: 'EEEV_HEL_PEPT',
      assemblyNames: ['NC_003899.1'],
      category: ['Genes'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_003899_1/YP_913812_1_EEEV_HEL_PEPT.gff3',
          }
      },
      "displays": [
          {
          "type": "LinearBasicDisplay",
          "displayId": "YP_913812_1_EEEV_HEL_PEPT-LinearBasicDisplay",
          "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" //"#34eb74"
          }
          }
      ]
    },
    {
      type: 'FeatureTrack',
      trackId: 'YP_913812_1_EEEV_PEPT',
      name: 'EEEV_PEPT',
      assemblyNames: ['NC_003899.1'],
      category: ['Genes'],
      adapter: {
          type: 'Gff3Adapter',
          gffLocation: {
          uri: '/assets/NC_003899_1/YP_913812_1_EEEV_PEPT.gff3',
          }
      },
      "displays": [
          {
          "type": "LinearBasicDisplay",
          "displayId": "YP_913812_1_EEEV_PEPT-LinearBasicDisplay",
          "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" //"#34eb74"
          }
          }
      ]
    },
    {
        type: 'FeatureTrack',
        trackId: 'YP_913813_1',
        name: 'EEEV_MACRO',
        assemblyNames: ['NC_003899.1'],
        category: ['Genes'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_003899_1/YP_913813_1.gff3',
            }
        }
        ,
        "displays": [
            {
            "type": "LinearBasicDisplay",
            "displayId": "YP_913813_1-LinearBasicDisplay",
            "renderer": {
                "type": "SvgFeatureRenderer",
                "color1": "jexl:colorFeature(feature)" //"#34eb74"
            }
            }
        ]
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740652_1',
        name: 'EEEV_RDRP',
        assemblyNames: ['NC_003899.1'],
        category: ['Genes'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
            uri: '/assets/NC_003899_1/NP_740652_1.gff3',
            }
        },
        displays: [
          {
          "type": "LinearBasicDisplay",
          "displayId": "NP_740652_1-LinearBasicDisplay",
          "renderer": {
              "type": "SvgFeatureRenderer",
              "color1": "jexl:colorFeature(feature)" //"#34eb74"
          }
          }
        ]
    },
    {
        type: 'FeatureTrack',
        trackId: 'EEEVgp1',
        name: 'EEEVgp1',
        assemblyNames: ['NC_003899.1'],
        category: ['Genes'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/EEEVgp1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740649_1',
        name: 'NP_740649.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740649_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740650_1',
        name: 'NP_740650.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740650_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740651_1',
        name: 'NP_740651.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740651_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'EEEVgp3',
        name: 'EEEVgp3',
        assemblyNames: ['NC_003899.1'],
        category: ['Genes'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/EEEVgp3.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740644_1',
        name: 'NP_740644.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740644_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'YP_913809_1',
        name: 'YP_913809.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/YP_913809_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740645_1',
        name: 'NP_740645.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740645_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740646_1',
        name: 'NP_740646.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740646_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740647_1',
        name: 'NP_740647.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740647_1.gff3',
            }
        }
    },
    {
        type: 'FeatureTrack',
        trackId: 'NP_740648_1',
        name: 'NP_740648.1',
        assemblyNames: ['NC_003899.1'],
        category: ['Protein'],
        adapter: {
            type: 'Gff3Adapter',
            gffLocation: {
                uri: '/assets/NC_003899_1/NP_740648_1.gff3',
            }
        }
    },
    */
  
];






