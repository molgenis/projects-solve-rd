<template>
  <page-component id="app">
    <div class="py-1 col-sm-8 m-auto" id="solverd-container">
      <section aria-labelledby="solverd-getstarted-title" class="mb-5" id="solverd-getstarted">
        <h1 id="solverd-getstarted-title">Get Started</h1>
        <p>Since the begining of RD3, data has been added to the data in periodic batches known as freezes. Each release is stored in its own location in the RD3 database and they all follow the same structure. All releases have the following tables.</p>
        <ul>
          <li><strong>Subjects</strong>: the subjects from whom the samples were collected</li>
          <li><strong>Subject Info</strong>: extra information about the subjects</li>
          <li><strong>Samples</strong>: list all the samples that were collected per subject</li>
          <li><strong>Lab Information</strong>: data on how the samples were processed</li>
          <li><strong>Files</strong>: location of the files that were generated</li>
        </ul>
        <p>If you would like to create a subset of the data based on a number of characteristics, use the <a href="https://rdnexus.molgeniscloud.org/Discover/Index">Discovery Nexus</a>.</p>
      </section>
      <section aria-labelledby="" class="mb-5" id="solverd-releases">
        <h2>Releases</h2>
        <p>Click the name of the release below to view all tables available in RD3. Follow the links to view the data. To view a specific patch, click a link and select the patch filter in the table filters panel.</p>
        <Accordion v-for="pkg in packages" :key="pkg.id" :id="pkg.id" :title="pkg.label" :links="data[pkg.id]"/>
      </section>
    </div>
  </page-component>
</template>

<script>
import PageComponent from '@molgenis/molgenis-ui-context/src/components/PageComponent.vue'
import Accordion from './components/Accordion.vue'
import axios from 'axios'

export default {
  data () {
    return {
      packages: [],
      data: {}
    }
  },
  components: { PageComponent, Accordion },
  mounted () {
    axios.all([
      this.getEmxPackages()
    ]).then(response => {
      this.getEmxEntities()
    })
  },
  methods: {
    async getEmxPackages () {
      const response = await axios.get('/api/v2/sys_md_Package?attrs=id,label&q=parent==rd3')
      response.data.items.forEach((d, i) => {
        return this.packages.push({ id: d.id, label: d.label })
      })
    },
    async getEmxEntities () {
      let filter = []
      this.packages.map(d => filter.push(d.id))
      filter = filter.toString()
      const url = `/api/v2/sys_md_EntityType?attrs=id,label,package&q=package=in=(${filter})`
      const response = await axios.get(url)
      response.data.items.forEach((d, i) => {
        if (!(d.package.id in this.data)) {
          this.data[d.package.id] = []
        }
        this.data[d.package.id].push({
          id: d.id,
          label: d.label,
          package: d.package.id
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
</style>
