<template>
  <Page>
    <main>
      <Section id="patient-tree-viz">
        <h2>Patient Tree</h2>
        <p>Exercitation reprehenderit aliquip amet qui esse cillum sint tempor ipsum cillum minim labore id irure. Esse laboris est Lorem quis. Tempor tempor qui aute qui amet aliquip in commodo elit eiusmod. Consectetur nisi mollit id pariatur enim tempor voluptate do ad tempor amet. Est cillum dolor nostrud adipisicing reprehenderit consectetur tempor reprehenderit incididunt. Culpa occaecat mollit culpa in. Proident elit reprehenderit pariatur laborum irure anim in consectetur excepteur.</p>
        <div class="loading" v-if="loading">
          <p>Loading....</p>
        </div>
        <TreeView id="patient-tree" :data="treeData" v-else/>
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
#patient-tree-viz-section {
  min-height: calc(100vh - 9em);
}
</style>
