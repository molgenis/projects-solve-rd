<template>
  <Page>
    <main>
      <Section id="patient-tree-viz">
        <h2>Patient Tree</h2>
        <p>The Patient Tree view was designed to link all samples and experiments across RD3 releases at the patient-level. Click a patient ID with the folder icon to view all linked samples. Continue clicking items until all items have been expanded.</p>
        <div class="loading" v-if="loading">
          <p>Loading....</p>
        </div>
        <TreeView id="patient-tree" :data="treeData.children" v-else/>
      </Section>
    </main>
  </Page>
</template>

<script>
import Page from '../components/Page.vue'
import Section from '../components/Section'
import TreeView from '../components/TreeView.vue'

export default {
  data () {
    return {
      loading: true,
      treeData: []
    }
  },
  components: {
    Page,
    Section,
    TreeView
  },
  methods: {
    async fetchData (url) {
      const response = await fetch(url)
      return response.json()
    }
  },
  mounted () {
    const endpoint = '/api/v2/rd3stats_json'
    Promise.all([
      this.fetchData(endpoint)
    ]).then(result => {
      const rawData = result[0]
      const treeData = rawData.items.map(row => {
        return row.id === 'patient-sample-viz' ? row.value : null
      })[0]
      this.treeData = JSON.parse(treeData)
    }).then(() => {
      this.loading = false
    })
  }
}

</script>

<style lang="scss">
// this is for demo purposes only
#patient-tree-viz {
  min-height: calc(100vh - 9em);
}
</style>
