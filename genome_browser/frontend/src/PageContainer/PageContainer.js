
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
//import Grid from '@mui/system/Unstable_Grid';
import Grid from '@mui/material/Grid';
import styled from '@mui/system/styled';


export default function PageContainer(props) {
  
  const [paths, setPaths] = useState([]);

  useEffect(() => {
    /*
    const fetchData = async () => {
      const req = await fetch('http://localhost:8080/api/species-list');
      let data = await req.json();
      setSpeciesList(data);
    }
    fetchData();
    */

    let pathArray = window.location.pathname.split('/');
    pathArray.shift(); 
    setPaths(pathArray);

  }, []);



  return (
    <Grid container rowSpacing={2}>
      <Grid xs={12} sx={{ p: 2 }} style={{background:"#fff", zIndex:3}}  >
        {props.header} 
      </Grid>
      <Grid container xs={12} spacing={0}>
        <Grid xs={2}> 
          {props.sideMenu} 
        </Grid>
        <Grid xs={10}>
          {props.mainContent}
        </Grid>
        {/*
        <Grid xs={2}>
          <Paper elevation={1} sx={{ p: 1 }}>
          {props.timeline}
          </Paper>
        </Grid>
        */}
      </Grid>
    </Grid>
  );
}

 
