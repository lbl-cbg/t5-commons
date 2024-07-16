

import * as React from 'react';
import { useState, useEffect, useRef } from 'react';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import Button from '@mui/material/Button'; 
import { Routes, Route, useParams } from 'react-router-dom';
import PageContainer from './PageContainer/PageContainer';
import { Outlet, Link as RouterLink } from "react-router-dom";
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import CircularProgress from '@mui/material/CircularProgress';
import HomeIcon from '@mui/icons-material/Home';
import Link from '@mui/material/Link';
import Typography from '@mui/material/Typography';
import CoronavirusIcon from '@mui/icons-material/Coronavirus';
import * as d3 from "d3";
import GenomeBrowser from "./jbrowse/components/GenomeBrowser";
import * as util from "./util";



export default function Species() {
   
  const { taxonId } = useParams();
  const [data, setData] = useState([]);
  /*const taxonidMap = {"11021":{abbr:"EEEV",
                                fullname:"Eastern equine encephalitis virus"
                              },
                      "11036":{abbr:"VEEV",
                                fullname:"Venezuelan equine encephalitis virus"
                              },
                      "37124":{abbr:"CHIKV",
                                fullname:"Chikungunya virus"
                              }
              }; */
  const [selectedMenuItem, setSelectedMenuItem] = useState("GenomeBrowser");
  const gbRef = useRef(null)
  const targetTableRef = useRef(null)
  
  const width = 500;
  const root = d3.hierarchy({
    "name": "flare",
    "children": [
      {
        "name": "analytics",
        "children": [
          {
            "name": "cluster",
            "children": [
              {"name": "AgglomerativeCluster", "size": 3938},
              {"name": "CommunityStructure", "size": 3812},
              {"name": "HierarchicalCluster", "size": 6714},
              {"name": "MergeEdge", "size": 743}
            ]
          }
        ]
      }
    ]
    });
  const dx = 10;
  const dy = width / (root.height + 1);

  // Create a tree layout.
  const tree = d3.cluster().nodeSize([dx, dy]);

  // Sort the tree and apply the layout.
  root.sort((a, b) => d3.ascending(a.data.name, b.data.name));
  tree(root);

  // Compute the extent of the tree. Note that x and y are swapped here
  // because in the tree layout, x is the breadth, but when displayed, the
  // tree extends right rather than down.
  let x0 = Infinity;
  let x1 = -x0;
  root.each(d => {
    if (d.x > x1) x1 = d.x;
    if (d.x < x0) x0 = d.x;
  });

  // Compute the adjusted height of the tree.
  const height = x1 - x0 + dx * 2;

  const svg = d3.create("svg")
      .attr("width", width)
      .attr("height", height)
      .attr("viewBox", [-dy / 3, x0 - dx, width, height])
      .attr("style", "max-width: 100%; height: auto; font: 10px sans-serif;");

  const link = svg.append("g")
      .attr("fill", "none")
      .attr("stroke", "#555")
      .attr("stroke-opacity", 0.4)
      .attr("stroke-width", 1.5)
    .selectAll()
      .data(root.links())
      .join("path")
        .attr("d", d3.linkHorizontal()
            .x(d => d.y)
            .y(d => d.x));
  
  const node = svg.append("g")
      .attr("stroke-linejoin", "round")
      .attr("stroke-width", 3)
    .selectAll()
    .data(root.descendants())
    .join("g")
      .attr("transform", d => `translate(${d.y},${d.x})`);

  node.append("circle")
      .attr("fill", d => d.children ? "#555" : "#999")
      .attr("r", 2.5);

  node.append("text")
      .attr("dy", "0.31em")
      .attr("x", d => d.children ? -6 : 6)
      .attr("text-anchor", d => d.children ? "end" : "start")
      .text(d => d.data.name)
    .clone(true).lower()
      .attr("stroke", "white");


  useEffect(() => {

    const fetchData = async () => {
      
      const req = await fetch('http://localhost:8080/api/species/'+taxonId);
      let data = await req.json();
      console.log("sr:",data); 
      setData(data);
    }

    fetchData();  
  }, []);
 

  const listItemClickHandler = (section) =>{
    console.log(".....",section);
    setSelectedMenuItem(section);
    if(section=='GenomeBrowser')
    {
      gbRef.current.scrollIntoView();
    }
    else if(section=='Targets')
    {
      targetTableRef.current.scrollIntoView({behavior: 'smooth'});
    }
  };


  return (
    <PageContainer
      header={
        <div>
          <Breadcrumbs aria-label="breadcrumb">
            <Link
              underline="hover"
              sx={{ display: 'flex', alignItems: 'center' }}
              color="inherit"
              component={RouterLink}
              to="/"
            >
              <HomeIcon sx={{ mr: 0.5 }} fontSize="inherit" />
            </Link>
            <Typography
              sx={{ display: 'flex', alignItems: 'center' }}
              color="text.primary"
            > 
              <CoronavirusIcon sx={{ mr: 0.5 }} fontSize="inherit" />
              {util.taxonidMap[taxonId].abbr}
            </Typography>
          </Breadcrumbs>

          <h2>
          {util.taxonidMap[taxonId].fullname}
          </h2>
          
        </div>
      }
      sideMenu={
        <Drawer 
          sx={{
          width: "10%",
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: "calc(100% * 2 / 13  )",
            boxSizing: 'border-box',
            zIndex:1,
            background: "unset",
            border:0,
            //position: 'absolute',
          },
          
          }}
          variant="permanent"
          anchor="left"
        >
          <div style={{height:"120px",minHeight:"100px"}}></div>
          <List> 
            <ListItem key={"GenomeBrowser"} disablePadding>
              <ListItemButton selected={selectedMenuItem=="GenomeBrowser"} onClick={() => listItemClickHandler('GenomeBrowser')}>
                <ListItemText primary={"Genome Browser"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"Targets"} disablePadding>
              <ListItemButton selected={selectedMenuItem=="Targets"} onClick={() => listItemClickHandler('Targets')}>
                <ListItemText primary={"Targets"} />
              </ListItemButton>
            </ListItem>
            {/*
            <ListItem key={"TreeView"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Tree View"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"Taxnomy"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Taxnomy"} />
              </ListItemButton>
            </ListItem>
            */}
          </List>
        </Drawer>
      }
      mainContent={
        <div>
          <span ref={gbRef}></span>
          <GenomeBrowser></GenomeBrowser>
          <br/>
          <Paper elevation={1} sx={{ p:1, mb:1 }} ref={targetTableRef}>
            <h3>Tagets</h3>
            <TableContainer >
              {(!data || data.length==0) && 
              <div style={{ display: 'flex', justifyContent: 'center' }}>
                <CircularProgress />
              </div>
              }
              {data && data.length>0 && 
              <Table aria-label="simple table">
                <TableHead>
                  <TableRow>
                    <TableCell>Org. Target ID</TableCell>
                    <TableCell>Target Annotation</TableCell> 
                    <TableCell>Vector</TableCell> 
                    <TableCell>Expression Level (%)</TableCell> 
                    <TableCell>Protein Concentration (mg/mL)</TableCell>  
                    <TableCell>Protein Volume (µL)</TableCell>
                    <TableCell>Buffer Content</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {data && data.map((row) => (
                    <TableRow
                      key={row.originaltargetid}
                      sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
                    >
                      <TableCell>
                        <RouterLink to={"target/"+row.originaltargetid}>{row.originaltargetid}</RouterLink>
                      </TableCell> 
                      <TableCell>{row.targetannotation}</TableCell>
                      <TableCell>{row.vector}</TableCell>
                      <TableCell>{row.expressionlevel}</TableCell>
                      <TableCell>{row.proteinconcentration}</TableCell>
                      <TableCell>{row.proteinvolume}</TableCell>
                      <TableCell>{row.buffercontent}</TableCell>
                    </TableRow>
                  ))}

                </TableBody>
              </Table>
              }
            </TableContainer>
          </Paper>
          {/*
          <Paper elevation={1} sx={{ p:1, mb:1 }}>
            <h3>Tree View</h3>
            <div style={{height:"500px"}}>
            </div>
          </Paper>
          */}
        </div>

      }
      /*
      timeline={
        <div>
          <h3>Timeline</h3>
          <div style={{height:"100%", display:"none", flexDirection:"column", color:"#000000de", fontSize:"14px"}}>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#04aa04"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div>STEP 1</div>
                <div><b>Genome Sequence</b></div>
                <div>09/01/2023</div>
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#04aa04"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div>STEP 2</div>
                <div><b>ID Protein Classification</b></div>
                <div>10/01/2023</div>
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#04aa04"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>Final Structural Orthologs</b></div>
                <div>10/10/2023</div>
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#fff", border:"1px solid #aaa"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>ID Complexes</b></div> 
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#04aa04"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>Design Constructs</b></div>
                <div>10/12/2023</div>
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#04aa04"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>Standardize Tags</b></div>
                <div>10/12/2023</div>
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#fff", border:"1px solid #aaa"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>SAXS</b></div> 
              </div>
            </div>


            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#fff", border:"1px solid #aaa"}}></div>
                <div style={{width:"2px",height:"80px",background:"#aaa"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>Crystallography</b></div> 
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#fff", border:"1px solid #aaa"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>Activity Assays</b></div> 
              </div>
            </div>

            <div style={{display:"flex", flexDirection:"row", alignItems:"start"}}>
              <div style={{display:"flex", flexDirection:"column", flex:"20%", alignItems:"center"}}>
                <div style={{height:"15px",width:"15px", borderRadius:"50%", background:"#fff", border:"1px solid #aaa"}}></div>
                <div style={{width:"2px",height:"80px",background:"#ccc"}}></div>
              </div>
              <div style={{display:"flex", flexDirection:"column", flex:"80%"}}>
                <div><b>Binding Assays</b></div> 
              </div>
            </div>







          </div>
        </div>
      }
      */
    >
    </PageContainer> 
  );
}

 
