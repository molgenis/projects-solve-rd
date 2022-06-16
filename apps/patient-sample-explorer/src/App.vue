<template>
  <div id="viz">
    <div v-if="loading">
      <p>loading...</p>
    </div>
    <div v-else>
      <TreeDiagram
        id="tree"
        :data="treeData"
      />
    </div>
  </div>
</template>

<script>
import TreeDiagram from './components/TreeDiagram.vue'
import './styles/styles.scss'

const data = {
  name: 'RD3',
  level: '0',
  children: [
    {
      id: 'm4RHac690hOV',
      name: 'P0001',
      group: 'patient',
      level: '1',
      children: [
        {
          id: 'Gy3zDzHtItqP',
          name: 'S0001',
          group: 'sample',
          level: '2',
          children: [
            {
              id: 'cqa58gEmFbsF',
              name: 'E0001',
              group: 'experiment',
              level: '3'
            },
            {
              id: 'WuRedxiP2FPO',
              name: 'E21410',
              group: 'experiment',
              level: '3'
            }
          ]
        },
        {
          id: 'szyc68wKK1OM',
          name: 'S0002',
          group: 'sample',
          level: '2',
          children: [
            {
              id: 'ykjDjI0eyZUr',
              name: 'E0003',
              group: 'experiment',
              level: '3'
            }
          ]
        }
      ]
    },
    {
      id: 'Ika2bq8ufxkK',
      name: 'P0002',
      level: '1'
    },
    {
      id: 'fyQ8Tzm4nUyq',
      name: 'P0003',
      group: 'patient',
      level: '1',
      children: [
        {
          id: 'T7AbwJEgXa1q',
          name: 'S0003',
          group: 'sample',
          level: '2'
        },
        {
          id: 'ywdPTVuVz9Uw',
          name: 'S0004',
          group: 'sample',
          level: '2',
          children: [
            {
              id: 'igQ7rk6UP2Ng',
              name: 'E0004',
              group: 'experiment',
              level: '3'
            },
            {
              id: 'C9o5pkFCtP1V',
              name: 'NO1244',
              group: 'experiment',
              level: '3'
            }
          ]
        }
      ]
    },
    {
      id: '6F6CXeQIkK8b',
      name: 'P0004',
      level: '1'
    },
    {
      id: 'ZGgNw9FNZ3cT',
      name: 'P0005',
      level: '1'
    }
  ]
}

export default {
  data () {
    return {
      loading: false,
      treeData: data
    }
  },
  components: {
    TreeDiagram
  }//,
  // methods: {
  //   async fetchData (url) {
  //     const response = await fetch(url)
  //     return response.json()
  //   }
  // },
  // mounted () {
  //   const endpoint = '/api/v2/rd3stats_json'
  //   Promise.all([
  //     this.fetchData(endpoint)
  //   ]).then(result => {
  //     const rawData = result[0]
  //     const treeData = rawData.items.map(row => {
  //       return row.id === 'patient-sample-viz' ? row.value : null
  //     })[0]
  //     this.treeData = JSON.parse(treeData)
  //   }).then(() => {
  //     this.loading = false
  //   })
  // }
}

</script>
