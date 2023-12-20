import React, { useState } from "react";
import {
  Container,
  AppBar,
  Tabs,
  Tab,
  Grid,
  Paper,
  Typography,
} from "@mui/material";

import TableRowsIcon from '@mui/icons-material/TableRows';
import BugReportIcon from '@mui/icons-material/BugReport';
import PlayCircleFilledWhiteIcon from '@mui/icons-material/PlayCircleFilledWhite';

import Todo from "./exercises/Debugging";
import DataTable from "./exercises/DataTable";

enum TabOption {
  intro = "Introduction",
  debugging = "Debugging",
  dataTable = "Data Table",
}

function App() {
  const [activeTab, setTab] = useState(TabOption.intro);

  return (
    <Container disableGutters>
      <Grid container direction="column" alignItems="center">
        <AppBar position="static" style={{ marginBottom: 25 }}>
          <Tabs indicatorColor="primary" textColor="inherit" value={activeTab}>
            <Tab
              label={TabOption.intro}
              icon={<PlayCircleFilledWhiteIcon />}
              iconPosition='start'
              onClick={() => setTab(TabOption.intro)}
              value={TabOption.intro}
            />
            <Tab
              label={TabOption.debugging}
              icon={<BugReportIcon />}
              iconPosition='start'
              value={TabOption.debugging}
              onClick={() => setTab(TabOption.debugging)}
            />
            <Tab
              label={TabOption.dataTable}
              icon={<TableRowsIcon />}
              iconPosition='start'
              value={TabOption.dataTable}
              onClick={() => setTab(TabOption.dataTable)}
            />
          </Tabs>
        </AppBar>
        {renderTab()}
      </Grid>
    </Container>
  );

  function renderTab(): React.ReactNode {
    switch (activeTab) {
      case TabOption.debugging:
        return <Todo />;
      case TabOption.dataTable:
        return <DataTable />;
      case TabOption.intro:
      default:
        return (
          <Paper style={{ minHeight: 400, minWidth: 500, padding: 25 }}>
            <Grid container direction="column">
              <Typography
                align="center"
                variant="h3"
                style={{ marginBottom: 25 }}
              >
                Axiom Data Science
              </Typography>

              <Typography
                align="center"
                variant="h6"
                style={{ marginBottom: 25 }}
              >
                React Data Table Exercises
              </Typography>

              <Typography paragraph component="div">
                Welcome and thank you for continuing the Axiom interview process!
              </Typography>

              <Typography paragraph component="div">
                We use this take-home coding challenge to supplement the interview process;
                it allows us to understand your abilities and experiences with different
                development tools and programming concepts without asking generic high
                level questions during a video interview. We have found it much more beneficial
                to let you showcase your abilities here, in actual code, as you would do if
                you were working here at Axiom.
              </Typography>

              <Typography
                align="center"
                variant="h4"
                style={{ margin: "25px 0px" }}
              >
                The Exercises
              </Typography>

              <Typography paragraph component="div">
                The tasks are as follows:
                <ol>
                  <li>
                    <b>Debugging:</b> Debug a broken application and make changes until it works as expected.
                  </li>
                  <li>
                    <b>Data Table:</b> Add new features to an existing application.
                  </li>
                </ol>
              </Typography>

              <Typography paragraph component="div">
                This should only take ~2 hours of time. Each exercise (using the
                tabs above) has its own description as well. Make your changes
                in the code to meet each challenge's tasks.
              </Typography>

              <Typography paragraph component="div">
                Please refer to the README in this repository for instructions on what
                to do if you have questions and how to submit your solutions.
              </Typography>

              <Typography
                align="center"
                variant="h5"
                style={{ marginTop: 25 }}
              >
                Good luck!
              </Typography>

            </Grid>
          </Paper>
        );
    }
  }
}

export default App;
