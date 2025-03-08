import React from 'react';
import ReactDOM from 'react-dom';
import { createRoot } from 'react-dom/client';
import {
  createBrowserRouter,
  RouterProvider,
} from "react-router-dom";
import './index.css';
import App from './App';
import Species from './Species';
import Target from './Target';




const router = createBrowserRouter([
  {
    path: "/",
    element: <App />,
  },
  {
    path: "species/:taxonId/target/:braveId",
    element: <Target />,
  },
  {
    path: "species/:taxonId",
    element: <Species />,
  },
]);

createRoot(document.getElementById("root")).render(
  
    <RouterProvider router={router} />
  
);





//const root = createRoot(document.getElementById('root'));
//root.render(<App />);


