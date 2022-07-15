import { createApp } from 'vue'
import { createRouter, createWebHistory } from 'vue-router'

import App from './App.vue'
import Home from './pages/Home.vue'
import GetStarted from './pages/GetStarted.vue'
import PatientTree from './pages/PatientTree.vue'

const routes = [
  {
    name: 'home',
    path: '/',
    component: Home
  },
  {
    name: 'getstarted',
    path: '/get-started',
    component: GetStarted
  },
  {
    name: 'patienttree',
    path: '/patient-tree',
    component: PatientTree
  }
]

const router = createRouter({
  history: createWebHistory(window.location.pathname),
  routes,
  scrollBehavior (to, from, savedPosition) {
    return {
      top: 0
    }
  }
})

const app = createApp(App)
app.use(router)
app.mount('#app')
