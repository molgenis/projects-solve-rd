<template>
  <div id="app">
    <div class="py-1 col-sm-8 m-auto" id="solverd-container">
      <section aria-labelledby="solverd-getstarted-title" id="solverd-getstarted">
        <h1 id="solverd-getstarted-title">Get Started</h1>
        <p>Since the start of RD3, data has been added to the database periodically. Each release, or data freeze, is stored in its own location within RD3. Patches, or an update to a data freeze, are stored within the relevant data freeze. You can view data from a specific patch by navigating to any data freeze table and subset the data using the patch filter.</p>
        <div class="row">
          <div class="col-sm-7">
            <p>There have been <strong>{{ Object.keys(packages).length }} releases</strong> to date (these are listed in the following section). All RD3 data freezes have an identical table structure. This table structure provides a standard format for structuring and storing data (see figure 1), as well as makes it easier to switch between the different data freezes. Tables are linked by one identifier that allows you to view referenced data in the browser. The samples and subject tables are linked by subject identifier. The samples, experiment, and files tables are linked by sample identifier.</p>
            <p>If you would like to create a subset of the data based on a number of characteristics, you can either 1) apply filters in the data viewer, 2) retrieve data via the API, or 3) use the <a href="https://rdnexus.molgeniscloud.org/Discover/Index">Discovery Nexus</a> platform.</p>
            <p>If you are experiencing difficulties or have any questions, please contact the Molgenis support desk: <a href="mailto:molgenis-support@umcg.nl">molgenis-support@umcg.nl</a>.</p>
          </div>
          <div class="col-sm-5" id="solverd-table-defs">
            <p class="title">Figure 1. RD3 Core Structure</p>
            <dl class="definition-list">
              <div>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  stroke-width="2"
                  aria-hidden="true"
                >
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    d="M5.121 17.804A13.937 13.937 0 0112 16c2.5 0 4.847.655 6.879 1.804M15 10a3 3 0 11-6 0 3 3 0 016 0zm6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
                <dt>
                  <span>Subjects</span>
                </dt>
                <dd>The individuals from whom the samples were collected</dd>
              </div>
              <div>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  stroke-width="2"
                  aria-hidden="true"
                >
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
                <dt>Subject Information</dt>
                <dd>Supplementary information about the subjects</dd>
              </div>
              <div>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  stroke-width="2"
                  aria-hidden="true"
                >
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"
                />
                </svg>
                <dt>Samples</dt>
                <dd>All of the samples that were collected from each subject</dd>
              </div>
              <div>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  stroke-width="2"
                  aria-hidden="true"
                >
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"
                  />
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"
                />
                </svg>
                <dt>Experiments</dt>
                <dd>Information about how the samples were processed</dd>
              </div>
              <div>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  stroke-width="2"
                  aria-hidden="true"
                >
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                />
                </svg>
                <dt>Files</dt>
                <dd>The location of the files that were generated</dd>
              </div>
            </dl>
          </div>
        </div>
      </section>
      <section aria-labelledby="solverd-releases-title" id="solverd-releases">
        <h2 id="solverd-releases-title">Releases</h2>
        <p>Click the name of the release below to view all tables available in RD3. Follow the links to view the data. If you would like to view a certain patch, click one of links below and select the "patch" filter.</p>
        <div class="col-md-8 m-auto">
          <Accordion
            v-for="pkg in packages"
            :key="pkg.id"
            :id="pkg.id"
            :title="pkg.label"
            :links="pkg.links"
          />
        </div>
      </section>
    </div>
  </div>
</template>
<script>
import Accordion from './components/Accordion.vue'
import axios from 'axios'

export default {
  data () {
    return {
      packages: {}
    }
  },
  components: {
    Accordion
  },
  mounted () {
    axios.all([
      this.getEmxPackages()
    ]).then(() => {
      this.getEmxEntities()
    })
  },
  methods: {
    async getEmxPackages () {
      const response = await axios.get('/api/v2/sys_md_Package?attrs=id,label&q=parent==rd3')
      response.data.items.forEach((data, index) => {
        if (!(data.id in this.packages)) {
          this.$set(this.packages, data.id, {})
        }
        this.$set(this.packages, data.id, { id: data.id, label: data.label })
      })
    },
    async getEmxEntities () {
      const filter = Object.keys(this.packages).toString()
      const url = `/api/v2/sys_md_EntityType?attrs=id,label,package&q=package=in=(${filter})`
      const response = await axios.get(url)
      response.data.items.forEach((data, index) => {
        if (!('links' in this.packages[data.package.id])) {
          this.$set(this.packages[data.package.id], 'links', [])
        }
        this.packages[data.package.id].links.push({
          id: data.id,
          label: data.label,
          package: data.package.id
        })
      })
    }
  }
}
</script>

<style lang="scss">
  body{
    height: 100%;
  }

  .definition-list {
    box-sizing: content-box;
    div {
      display: grid;
      grid-template-columns: 32px 2fr;
      grid-auto-rows: repeat(1fr, 2);
      justify-content: center;
      margin-bottom: 6px;

      $icon-size: 19px;
      svg {
        grid-column: 1;
        grid-row: 1 / 2;
        margin: auto;
        width: $icon-size;
        height: $icon-size;
      }

      dt {
        grid-column: 2;
        grid-row: 1 / 1;
        font-size: 11pt;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: bold;
      }

      dd {
        grid-column: 2;
        grid-row: 2 / 2;
        color: #3f454b;
      }
    }
  }

  #solverd-table-defs {
    background-color: #f6f6f6;
    padding: 24px;
  }
</style>
