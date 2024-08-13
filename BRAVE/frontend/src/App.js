
import "./App.css";
import '@fontsource/roboto/300.css';
import '@fontsource/roboto/400.css';
import '@fontsource/roboto/500.css';
import '@fontsource/roboto/700.css';
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
import { Outlet, Link } from "react-router-dom";

function App() {
   
  const [speciesList, setSpeciesList] = useState([]);

  useEffect(() => {

    const fetchData = async () => {
      const req = await fetch('http://localhost:8080/api/species-list');
      let data = await req.json(); 
      setSpeciesList(data);
    }

    fetchData();
  }, []);




  return (
    <div className="container">
      <header style={{background:"#fff", padding:"16px", marginBottom:"20px"}}>
        <h3>T5 Knowledge Base</h3>
      </header>


      <TableContainer component={Paper} sx={{ width:600 }} style={{alignSelf: "center"}}>
        <Table sx={{  minWidth: 500 }} aria-label="simple table">
          <TableHead>
            <TableRow>
              <TableCell>Taxon Id</TableCell>
              <TableCell>Species</TableCell> 
            </TableRow>
          </TableHead>
          <TableBody>
            {speciesList.map((row) => (
              <TableRow
                key={row.taxon_id}
                sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
              >
                <TableCell component="th" scope="row">
                  {row.taxon_id}
                </TableCell>
                <TableCell>
                  <Link to={`species/`+row.taxon_id}>{row.species}</Link>
                  
                </TableCell> 
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <br/>
      <br/>

    </div>
  );
}

export default App;
