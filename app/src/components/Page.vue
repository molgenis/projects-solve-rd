<template>
  <div class="page">
    <slot></slot>
    <div class="page__footer">
      <div class="footer__navigation">
        <img
          :src="require('@/assets/solve-rd-logo.png')"
          alt="Solve-RD - solving the unsolved rare diseases"
          class="solve-rd-logo"
        />
        <img
          :src="require('@/assets/molgenis-logo-blue-small.png')"
          alt="molgenis open source data platform" class="molgenis_logo"
        />
      </div>
      <p class="molgenis-citation">This database was created using the open source MOLGENIS software {{ molgenisVersion }} on {{ molgenisBuildDate }}</p>
    </div>
  </div>
</template>

<script>
import { fetchData } from '@/utils/search'

export default {
  name: 'ui-page',
  data () {
    return {
      molgenisVersion: null,
      molgenisBuildDate: null
    }
  },
  methods: {
    getAppContext () {
      Promise.all([
        fetchData('/app-ui-context')
      ]).then(response => {
        const uiContext = response[0]
        this.molgenisVersion = uiContext.version
        this.molgenisBuildDate = uiContext.buildDate
      })
    }
  },
  mounted () {
    this.getAppContext()
  }
}
</script>

<style lang="scss">
.mg-page,
.mg-app,
.container-fluid,
.row,
.col-sm-8,
.col-sm-3 {
  padding: 0;
}

.mg-page .mg-page-content {
  margin-top: 0;
}

footer.footer {
  display: none;
}

.page {
  min-height: 100vh;
  background-color: #f6f6f6;
  
  .page__footer {
    box-sizing: padding-box;
    padding: 2em;
    background-color: #282d32;
    color: #f6f6f6;
    
    .footer__navigation {
      display: flex;
      justify-content: flex-start;
      align-items: center;
      gap: 1.25em;
    }
    
    .molgenis-citation {
      font-size: 10pt;
      margin-top: 16px;
      color: #bfbfbf;
    }
    
    .molgenis_logo {
      width: 124px;
    }
    
    .solve-rd-logo {
      width: 172px;
    }
  }
}
</style>
