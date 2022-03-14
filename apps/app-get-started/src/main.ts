import Vue from 'vue'
import App from './App.vue'
import '@molgenis-ui/components-library/dist/components-library.css'
import { BootstrapVue } from 'bootstrap-vue'
// import { FontAwesomeIcon } from '@fortawesome/vue-fontawesome'
// import { library } from '@fortawesome/fontawesome-svg-core'
// import { faSort, faShare, faSortAlphaUp, faSortAlphaDown } from '@fortawesome/free-solid-svg-icons'

// library.add(faSort, faShare, faSortAlphaUp, faSortAlphaDown)

// Vue.use(BootstrapVue)
// Vue.component('FontAwesomeIcon', FontAwesomeIcon)
Vue.prototype.$eventBus = new Vue()
Vue.config.productionTip = false

const i18n = {
  install (Vue:any, options:any) {
    Vue.prototype.$t = (str:any) => {
      return str
    }
  }
}
Vue.use(i18n)

new Vue({
  render: h => h(App)
}).$mount('#app')
