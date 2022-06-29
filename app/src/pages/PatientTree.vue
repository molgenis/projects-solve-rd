<template>
  <Page>
    <main>
      <Section id="patient-tree-viz">
        <h2>Patient Tree</h2>
        <p>The <strong>Patient Tree</strong> visualizes the connection between patients, samples, and experiments. Search for records using one or more subject- or family identifiers. At the top level, are patients. Click a patient ID to view all linked samples. Click a sample ID to view all linked experiments.</p>
        <Errorbox v-if="requestHasFailed">
          <p class="text-error">Unable to retrieve data due to:</p>
          {{ error }}
        </Errorbox>
        <p v-else-if="loading && !requestHasFailed">Loading data....</p>
        <TreeView id="patient-tree" :data="treedata" v-else/>
      </Section>
    </main>
  </Page>
</template>

<script>
import Page from '../components/Page.vue'
import Section from '../components/Section'
import TreeView from '../components/TreeView.vue'
import Errorbox from '../components/Errorbox.vue'

export default {
  name: 'page-patient-tree',
  components: {
    Page,
    Section,
    Errorbox,
    TreeView
  },
  data () {
    return {
      loading: true,
      requestHasFailed: false,
      error: null,
      treedata: []
    }
  },
  methods: {
    async fetchData (url) {
      const response = await fetch(url)
      return response.json()
    }
  },
  mounted () {
    const endpoint = '/api/v2/rd3stats_treedata?attributes=id,json'
    Promise.all([
      this.fetchData(endpoint)
    ]).then(result => {
      const treedata = result[0].items
      this.treedata = treedata
    }).then(() => {
      this.loading = false
    }).catch(error => {
      this.requestHasFailed = true
      this.error = error
    })
  }
}

</script>

<style lang="scss">
// this is for demo purposes only
#patient-tree-viz {
  min-height: calc(100vh - 9em);
  
  .tree__icon__patient {
    fill: #DEAF02
  }
  
  .tree__icon__sample {
    fill: #478DAE;
  }
}
</style>
