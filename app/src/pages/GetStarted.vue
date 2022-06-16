<template>
  <Page>
    <Header
      id="getstarted-header"
      title="Getting Started with RD3"
      subtitle="Learn more and how to find data"
    />
    <Section id="getstarted-links" aria-labelledby="getstarted-links-title" class="text-center">
      <h2 id="getstarted-links-title">Quick Links</h2>
      <p>Scroll down to learn more about RD3 or use the links below to view a specific page.</p>
      <div class="action-link-container">
        <ActionLink href="/#/patient-tree">Patient Tree</ActionLink>
        <ActionLink href="/menu/main/dataexplorer?entity=rd3_overview&hideselect=true" :showExternalLinkIcon="true">Go to Data Overview</ActionLink>
        <ActionLink href="https://rdnexus.molgeniscloud.org/Discover/Index" :showExternalLinkIcon="true">Go to Discovery Nexus</ActionLink>
      </div>
    </Section>
    <Section id="getstarted-learnmore" aria-labelledby="getstarted-learnmore-title" class="section-bg-plain">
      <h2 id="getstarted-learnmore-title">About RD3</h2>
      <p>Since the start of RD3, data has been added to the database periodically from a number of sources (see image below). Initial metadata on subjects, samples, and experiments is provided by CNAG. The EGA provides further metadata (files, phenotypic- and diagnostic information) by transfering files to the sandbox environment. Raw data is imported into one of the portal tables where it is processed and moved into the appropriate release tables. Each release, or data freeze, is stored in its own location within RD3 and is an instance of the "release core structure" (or a set of standard tables). Patches, or an update to a data freeze, are stored within the relevant data freeze.</p>
      <Image
        :src="images.rd3Overview"
        alt="rd3 overview on the transfer of data between CNAG, EGA, and the sandbox"
      />
      <h3 id="getstarted-finddata-title">Finding data</h3>
      <p>There are several options available for finding data in RD3 from using a query interface to interacting with RD3 via the API. Depending on your choice, you may need to additional permissions. If you are experiencing difficulties or have any questions, please contact the Molgenis support desk: <a href="mailto:molgenis-support@umcg.nl">molgenis-support@umcg.nl</a>.</p>
      <ul>
        <li><a href="https://rdnexus.molgeniscloud.org/Discover/Index">Discovery Nexus platform</a>: this tool allows you to build cohorts based on ERN, phenotypic information, diseases, genes and pathways, and more.</li>
        <li><a href="/menu/main/dataexplorer?entity=rd3_overview&hideselect=true">RD3 Data Overview table</a>: this table combines all subjects and the samples, experiments, and files across all releases.</li>
        <li><strong>Work in a specific release</strong>: If you know which RD3 release you would like to work with (e.g., Data Freeze 2, Novel Omics Deep-WES, etc.), you can access the tables using the links provided in the next section.</li>
        <li><strong>Molgenis API</strong>: If you would rather retrieve data as part of a workflow or from a script, you can do so using the Molgenis API. You can use one of the existing clients (R, Python, JavaScript) or send requests using curl. Additional permissions are required. Please contact the Molgenis support desk to get started: <a href="mailto:molgenis-support@umcg.nl">molgenis-support@umcg.nl</a>.</li>
      </ul>
    </Section>
    <Section id="getstarted-releases" aria-labelledby="getstarted-releases-title" class="section-bg-plain">
      <h2 id="getstarted-releases-title">Current RD3 Releases</h2>
      <p>There have been <strong>{{ Object.keys(packages).length }} releases</strong> to date. All RD3 data freezes have an identical table structure. This table structure provides a standard format for structuring and storing data, as well as makes it easier to switch between the different data freezes. Tables are linked by one or more identifiers so you can view referenced data in the browser.</p>
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
    </Section>
  </Page>
</template>

<script>
import Page from '../components/Page.vue'
import Header from '../components/Header.vue'
import Section from '../components/Section.vue'
import ActionLink from '../components/ActionLink.vue'
import Accordion from '../components/Accordion.vue'
import Image from '../components/Image.vue'

export default {
  data () {
    return {
      packages: {},
      images: {
        rd3Overview: require('../assets/rd3-data-flow.png')
      }
    }
  },
  components: {
    Page,
    Header,
    Section,
    ActionLink,
    Accordion,
    Image
  },
  methods: {
    async fetch (url) {
      const response = await fetch(url)
      const data = await response.json()
      return data
    },
    extractEmxPackages (data) {
      return data.reduce((accumulator, row) => {
        accumulator[row.id] = {
          id: row.id,
          label: row.label
        }
        return accumulator
      }, {})
    },
    extractEmxEntities (data) {
      data.items.forEach(row => {
        if (!('links' in this.packages[row.package.id])) {
          this.packages[row.package.id].links = []
        }
        this.packages[row.package.id].links.push({
          id: row.id,
          label: row.label,
          package: row.package.id
        })
      })
    }
  },
  mounted () {
    Promise.all([
      this.fetch('/api/v2/sys_md_Package?attrs=id,label&q=parent==rd3')
    ]).then(packages => {
      this.packages = this.extractEmxPackages(packages[0].items)
      const filter = Object.keys(this.packages).toString()
      const url = `/api/v2/sys_md_EntityType?attrs=id,label,package&q=package=in=(${filter})`
      return this.fetch(url)
    }).then((result) => {
      this.extractEmxEntities(result)
    })
  }
}
</script>

<style lang="scss">
  .mg-page .mg-page-content {
    margin-top: 0;
  }
  
  #getstarted-links-section {
    background-color: #f6f6f6;
  }
  
  .action-link-container {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    gap: 1em;
    justify-content: center;
  }

  #solverd-table-defs {
    background-color: #f6f6f6;
    padding: 24px;
    width: 90%;
    margin: 16px auto;
    
    @media screen and (min-width: 925px) {
      max-width: 500px;
    }
  }
</style>
