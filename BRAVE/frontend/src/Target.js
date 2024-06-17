

import * as React from 'react';
import { useState, useEffect } from 'react';
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
import HomeIcon from '@mui/icons-material/Home';
import Link from '@mui/material/Link';
import Typography from '@mui/material/Typography';
import CoronavirusIcon from '@mui/icons-material/Coronavirus';


export default function Target() {
   
  const { taxonId, braveId } = useParams();
  const [data, setData] = useState({});
  const taxonidMap = {"11021":"EEEV",
                      "11036":"VEEV",
                      "37124":"CHIKV"
              };

  useEffect(() => {

    const fetchData = async () => {
      
      const req = await fetch('http://localhost:8080/api/species/'+taxonId+"/target/"+braveId);
      let data = await req.json();
      console.log("tr",data);
      setData(data);
    }

    fetchData();
  }, []);




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
            <Link
              underline="hover"
              sx={{ display: 'flex', alignItems: 'center' }}
              color="inherit"
              component={RouterLink}
              to={"/species/"+taxonId}
            > 
              <CoronavirusIcon sx={{ mr: 0.5 }} fontSize="inherit" />
              {taxonidMap[taxonId]}
            </Link>
            <Typography
              sx={{ display: 'flex', alignItems: 'center' }}
              color="text.primary"
            > 
              {braveId}
            </Typography>
          </Breadcrumbs>

          <h2>
            {data.brave_id} - {data.af2_id}
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
            border:0
          },
          
          }}
          variant="permanent"
          anchor="left"
        >
          <div style={{height:"150px",minHeight:"150px"}}></div>
          <List> 
            <ListItem key={"Sequences"} disablePadding>
              <ListItemButton selected={true}>
                <ListItemText primary={"Sequences"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"StructurePrediction"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Structure Prediction"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"ProteinPurification"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Protein Purification"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"Shipments"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Shipments"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"SAXS"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"SAXS"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"Crystallography"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Crystallography"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"ochar"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Other Characterizations / Models"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"com"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Compound & Inhibitors"} />
              </ListItemButton>
            </ListItem>
            <ListItem key={"Assays"} disablePadding>
              <ListItemButton>
                <ListItemText primary={"Assays"} />
              </ListItemButton>
            </ListItem>
          </List>
        </Drawer>
      }
      mainContent={
        <div>
          <Paper elevation={1} sx={{ p:2, mb:1 }}>
            <h3>Sequences</h3>
            <h4>Protein Sequence</h4>
            <p style={{ wordWrap: "break-word"}}>{data.proteinseq}</p>
            <h4>TreeViewST DNA Sequence</h4>
            <p style={{ wordWrap: "break-word"}}>{data.twist_dnaseq}</p>
          </Paper>
          <Paper elevation={1} sx={{ p:2, mb:1 }}>
            <h3>Structure Prediction</h3> 
            <img src={"/assets/"+ data.af2_id +"/"+data.af2_id+"_plddt.png"} style={{width:"100%"}}></img>
          </Paper>
        </div>

      }
      timeline={
        <div>
          <h3>Timeline</h3>
          <div style={{height:1000}}></div>
        </div>
      }
    >
    </PageContainer> 
  );
}

 
