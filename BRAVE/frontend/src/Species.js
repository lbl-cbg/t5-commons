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
import { Outlet, Link as RouterLink } from 'react-router-dom';
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
import * as d3 from 'd3';
import GenomeBrowser from './jbrowse/components/GenomeBrowser';

export default function Species() {
  const { taxonId } = useParams();
  const [data, setData] = useState({});
  const taxonidMap = { 11021: 'EEEV', 11036: 'VEEV', 37124: 'CHIKV' };

  const width = 500;
  const root = d3.hierarchy({
    name: 'flare',
    children: [
      {
        name: 'analytics',
        children: [
          {
            name: 'cluster',
            children: [
              { name: 'AgglomerativeCluster', size: 3938 },
              { name: 'CommunityStructure', size: 3812 },
              { name: 'HierarchicalCluster', size: 6714 },
              { name: 'MergeEdge', size: 743 },
            ],
          },
        ],
      },
    ],
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
  root.each((d) => {
    if (d.x > x1) x1 = d.x;
    if (d.x < x0) x0 = d.x;
  });

  // Compute the adjusted height of the tree.
  const height = x1 - x0 + dx * 2;

  const svg = d3
    .create('svg')
    .attr('width', width)
    .attr('height', height)
    .attr('viewBox', [-dy / 3, x0 - dx, width, height])
    .attr('style', 'max-width: 100%; height: auto; font: 10px sans-serif;');

  const link = svg
    .append('g')
    .attr('fill', 'none')
    .attr('stroke', '#555')
    .attr('stroke-opacity', 0.4)
    .attr('stroke-width', 1.5)
    .selectAll()
    .data(root.links())
    .join('path')
    .attr(
      'd',
      d3
        .linkHorizontal()
        .x((d) => d.y)
        .y((d) => d.x)
    );

  const node = svg
    .append('g')
    .attr('stroke-linejoin', 'round')
    .attr('stroke-width', 3)
    .selectAll()
    .data(root.descendants())
    .join('g')
    .attr('transform', (d) => `translate(${d.y},${d.x})`);

  node
    .append('circle')
    .attr('fill', (d) => (d.children ? '#555' : '#999'))
    .attr('r', 2.5);

  node
    .append('text')
    .attr('dy', '0.31em')
    .attr('x', (d) => (d.children ? -6 : 6))
    .attr('text-anchor', (d) => (d.children ? 'end' : 'start'))
    .text((d) => d.data.name)
    .clone(true)
    .lower()
    .attr('stroke', 'white');

  useEffect(() => {
    const fetchData = async () => {
      const req = await fetch('/api/species/' + taxonId);
      let data = await req.json();
      console.log('sr:', data);
      setData(data);
    };

    fetchData();
  }, []);

  // const bbb = (e)=>{
  //   console.log("VVVSSS:", viewState.session);
  // };

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
              <HomeIcon
                sx={{ mr: 0.5 }}
                fontSize="inherit"
              />
            </Link>
            <Typography
              sx={{ display: 'flex', alignItems: 'center' }}
              color="text.primary"
            >
              <CoronavirusIcon
                sx={{ mr: 0.5 }}
                fontSize="inherit"
              />
              {taxonidMap[taxonId]}
            </Typography>
          </Breadcrumbs>

          <h2>{data.species}</h2>
        </div>
      }
      sideMenu={
        <Drawer
          sx={{
            width: '10%',
            flexShrink: 0,
            '& .MuiDrawer-paper': {
              width: 'calc(100% * 2 / 13  )',
              boxSizing: 'border-box',
              zIndex: 1,
              background: 'unset',
              border: 0,
            },
          }}
          variant="permanent"
          anchor="left"
        >
          <div style={{ height: '150px', minHeight: '150px' }}></div>
          <List>
            <ListItem
              key={'Targets'}
              disablePadding
            >
              <ListItemButton selected={true}>
                <ListItemText primary={'Targets'} />
              </ListItemButton>
            </ListItem>
            <ListItem
              key={'TreeView'}
              disablePadding
            >
              <ListItemButton>
                <ListItemText primary={'Tree View'} />
              </ListItemButton>
            </ListItem>
            <ListItem
              key={'Taxnomy'}
              disablePadding
            >
              <ListItemButton>
                <ListItemText primary={'Taxnomy'} />
              </ListItemButton>
            </ListItem>
          </List>
        </Drawer>
      }
      mainContent={
        <div>
          <GenomeBrowser></GenomeBrowser>
          <br />
          <Paper
            elevation={1}
            sx={{ p: 1, mb: 1 }}
          >
            <h3>Tagets</h3>
            <TableContainer>
              <Table aria-label="simple table">
                <TableHead>
                  <TableRow>
                    <TableCell>Target ID</TableCell>
                    <TableCell>NI (%)</TableCell>
                    <TableCell>EXP (%)</TableCell>
                    <TableCell>SOL (%)</TableCell>
                    <TableCell>CONC (mg/mL)</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {data.target_summary &&
                    data.target_summary.map((row) => (
                      <TableRow
                        key={row.target_id}
                        sx={{
                          '&:last-child td, &:last-child th': { border: 0 },
                        }}
                      >
                        <TableCell>
                          <RouterLink to={'target/' + row.target_id}>
                            {row.target_id}
                          </RouterLink>
                        </TableCell>
                        <TableCell>{row.ni}</TableCell>
                        <TableCell>{row.exp}</TableCell>
                        <TableCell>{row.sol}</TableCell>
                        <TableCell>{row.conc}</TableCell>
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
          <Paper
            elevation={1}
            sx={{ p: 1, mb: 1 }}
          >
            <h3>Tree View</h3>
            <div style={{ height: '500px' }}></div>
          </Paper>
        </div>
      }
      timeline={
        <div>
          <h3>Timeline</h3>
          <div
            style={{
              height: '100%',
              display: 'none',
              flexDirection: 'column',
              color: '#000000de',
              fontSize: '14px',
            }}
          >
            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#04aa04',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>STEP 1</div>
                <div>
                  <b>Genome Sequence</b>
                </div>
                <div>09/01/2023</div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#04aa04',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>STEP 2</div>
                <div>
                  <b>ID Protein Classification</b>
                </div>
                <div>10/01/2023</div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#04aa04',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>Final Structural Orthologs</b>
                </div>
                <div>10/10/2023</div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#fff',
                    border: '1px solid #aaa',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>ID Complexes</b>
                </div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#04aa04',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>Design Constructs</b>
                </div>
                <div>10/12/2023</div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#04aa04',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>Standardize Tags</b>
                </div>
                <div>10/12/2023</div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#fff',
                    border: '1px solid #aaa',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>SAXS</b>
                </div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#fff',
                    border: '1px solid #aaa',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#aaa' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>Crystallography</b>
                </div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#fff',
                    border: '1px solid #aaa',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>Activity Assays</b>
                </div>
              </div>
            </div>

            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'start',
              }}
            >
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '20%',
                  alignItems: 'center',
                }}
              >
                <div
                  style={{
                    height: '15px',
                    width: '15px',
                    borderRadius: '50%',
                    background: '#fff',
                    border: '1px solid #aaa',
                  }}
                ></div>
                <div
                  style={{ width: '2px', height: '80px', background: '#ccc' }}
                ></div>
              </div>
              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  flex: '80%',
                }}
              >
                <div>
                  <b>Binding Assays</b>
                </div>
              </div>
            </div>
          </div>
        </div>
      }
    ></PageContainer>
  );
}
